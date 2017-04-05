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
            .format("INSERT INTO %s.%s (key,rev,tid,value) VALUES (?,?,now(),?)", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    protected final CassandraSession session;
    protected final int concurrency;
    protected final int numPartitions;
    protected final int partitionStart;
    protected final int numRevisions;
    protected final int revisionStart;
    protected final int numRuns;
    protected final int queueSize;
    protected final ByteBuffer value;
    protected final ExecutorService executor;
    protected final Meter attempts;
    protected final Meter failures;
    protected int runCount;

    public Writer(
            MetricRegistry metrics,
            CassandraSession session,
            int concurrency,
            int numPartitions,
            int partOffset,
            int numRevisions,
            int revOffset,
            int numRuns) throws IOException {
        this.session = checkNotNull(session);
        this.concurrency = concurrency;
        this.numPartitions = numPartitions;
        this.partitionStart = partOffset;
        this.numRevisions = numRevisions;
        this.revisionStart = revOffset;
        this.numRuns = numRuns;

        this.queueSize = this.concurrency * 2;
        this.attempts = metrics.meter(name(Writer.class, "all"));
        this.failures = metrics.meter(name(Writer.class, "failed"));

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(this.queueSize);

        metrics.register(name(Writer.class, "enqueued"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return queue.size();
            }
        });

        metrics.register(name(Writer.class, "runs"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return runCount;
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

    public void execute() {
        PreparedStatement prepared = this.session.prepare(QUERY);
        
        for (int i = 0; i < this.numRuns; i++) {
            for (int j = this.revisionStart; j < (this.numRevisions + this.revisionStart); j++) {
                for (int k = this.partitionStart; k < (this.numPartitions + this.partitionStart); k++) {
                    final String key = keyName(k);
                    final int rev = j;
                    executor.submit(() -> {
                        Statement statement = null;
                        try {
                            statement = prepared.bind(key, rev, value);
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
            this.runCount++;
        }
        LOG.info("All inserts enqueued; Shutting down...");
        executor.shutdown();

        try {
            // Block until any jobs on the queue have completed, or the timeout has expired.
            if (!executor.awaitTermination(Math.max(this.queueSize, 180), TimeUnit.SECONDS)) {
                LOG.warn("Timed out waiting for executor shutdown!");
            }
        }
        catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for executor shutdown", e);
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
