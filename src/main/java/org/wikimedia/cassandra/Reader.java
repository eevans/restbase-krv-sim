package org.wikimedia.cassandra;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.wikimedia.cassandra.CassandraSession.KEYSPACE;
import static org.wikimedia.cassandra.CassandraSession.TABLE;

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

@Command(name = "read", description = "Read data")
public class Reader {
    private static final String QUERY = String.format("SELECT rev,tid,value FROM %s.%s WHERE key = ?", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

    private final CassandraSession session;
    private final int concurrency;
    private final int numPartitions;
    private final int partitionStart;
    private final PreparedStatement prepared;
    private final ExecutorService executor;
    private final Meter attempts;
    private final Meter failures;

    public Reader(MetricRegistry metrics, CassandraSession session, int concurrency, int numPartitions, int partOffset) {
        this.concurrency = concurrency;
        this.session = checkNotNull(session);
        this.numPartitions = numPartitions;
        this.partitionStart = partOffset;
        this.prepared = session.prepare(QUERY);

        this.attempts = metrics.meter(name(Writer.class, "selects", "attempted"));
        this.failures = metrics.meter(name(Writer.class, "selects", "failed"));

        // FIXME: Concurrency, (and queue size?) should be configurable
        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(5000);

        metrics.register(name(Writer.class, "selects", "enqueued"), new Gauge<Integer>() {
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
    }

    public void execute() throws InterruptedException {
        for (int k = this.partitionStart; k < (this.numPartitions + this.partitionStart); k++) {
            final String key = Writer.keyName(k);
            executor.submit(() -> {
                Statement statement = null;
                try {
                    statement = this.prepared.bind(key);
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
        LOG.info("All inserts enqueued; Shutting down...");
        executor.shutdown();

        // This timeout needs to be long enough to accommodate the number of enqueued requests (relates to the
        // size of the blocking queue backing the thread-pool).
        if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
            LOG.error("Timed out waiting for executor shutdown!");
        }

    }

}
