package org.wikimedia.cassandra;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.wikimedia.cassandra.CassandraSession.KEYSPACE;
import static org.wikimedia.cassandra.CassandraSession.TABLE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.github.rvesse.airline.annotations.Command;

@Command(name = "write", description = "Write data")
public class Writer {
    private static final String QUERY = String
            .format("INSERT INTO %s.%s (key,rev,tid,value) VALUES (?,?,now(),?) USING TTL ?", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final CassandraSession session;
    private final int concurrency;
    private final int numPartitions;
    private final int partitionStart;
    private final int numRevisions;
    private final int revisionStart;
    private final int numRenders;
    private final PreparedStatement prepared;
    private final ByteBuffer value;
    private final ExecutorService executor;
    private final Meter attempts;
    private final Meter failures;

    public Writer(
            MetricRegistry metrics,
            CassandraSession session,
            int concurrency,
            int numPartitions,
            int partOffset,
            int numRevisions,
            int revOffset,
            int numRenders) throws IOException {
        this.session = checkNotNull(session);
        this.concurrency = concurrency;
        this.numPartitions = numPartitions;
        this.partitionStart = partOffset;
        this.numRevisions = numRevisions;
        this.revisionStart = revOffset;
        this.numRenders = numRenders;
        this.prepared = session.prepare(QUERY);

        // FIXME: Concurrency, (and queue size?) should be configurable
        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(5000);

        metrics.register(name(Writer.class, "inserts", "enqueued"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return queue.size();
            }
        });

        this.executor = new ThreadPoolExecutor(
                this.concurrency,
                this.concurrency,
                30,
                TimeUnit.SECONDS,
                queue,
                new ThreadPoolExecutor.CallerRunsPolicy());

        // Read in sample data
        this.value = bytes(getClass().getResourceAsStream("/foobar.html"));
    }

    public void execute() throws InterruptedException {
        for (int i = this.revisionStart; i < (this.numRevisions + this.revisionStart); i++) {
            for (int j = 0; j < this.numRenders; j++) {
                for (int k = this.partitionStart; k < (this.numPartitions + this.partitionStart); k++) {
                    final String key = keyName(k);
                    final int rev = i;
                    final int ttl = j < (this.numRenders - 1) ? 60 : 0; // FIXME: maybe not hard-code TTL?
                    executor.submit(() -> {
                        Statement statement = null;
                        try {
                            statement = this.prepared.bind(key, rev, value, ttl);
                            this.session.execute(statement);
                            this.attempts.mark();
                        }
                        catch (NoHostAvailableException | QueryExecutionException e) {
                            LOG.warn(e.getMessage());
                            this.failures.mark();
                        }
                        catch (Exception e) {
                            LOG.error("Unable to execute statement: \"{}\"", statement, e);
                            this.failures.mark();
                        }
                    });
                }
            }
        }
        LOG.info("All inserts enqueued; Shutting down...");
        executor.shutdown();

        // This timeout needs to be long enough to accommodate the number of enqueued requests (relates to the
        // size of the blocking queue backing the thread-pool).

        if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
            LOG.error("Timed out waiting for executor shutdown!");
        }
    }

    static String keyName(int sequence) {
        return String.format("key_%d", sequence);
    }

    static ByteBuffer bytes(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
        int length;
        while ((length = input.read(buff)) != -1)
            output.write(buff, 0, length);
        return ByteBuffer.wrap(output.toByteArray());
    }
}
