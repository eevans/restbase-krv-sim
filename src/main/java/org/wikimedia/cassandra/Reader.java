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
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.github.rvesse.airline.annotations.Command;

@Command(name = "read", description = "Read data")
public class Reader {
    private static final ConsistencyLevel CL = ConsistencyLevel.LOCAL_QUORUM;
    private static final String QUERY = String.format("SELECT rev,tid,value FROM %s.%s WHERE key=? LIMIT 1", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

    protected final CassandraSession session;
    protected final int concurrency;
    protected final int numPartitions;
    protected final int partitionStart;
    protected final int numRuns;
    protected final int queueSize;
    protected final ExecutorService executor;
    protected final Meter failures;
    protected final Timer reads;

    protected int runCount;

    public Reader(
            MetricRegistry metrics,
            CassandraSession session,
            int concurrency,
            int numPartitions,
            int partOffset,
            int numRuns) {
        this.concurrency = concurrency;
        this.session = checkNotNull(session);
        this.numPartitions = numPartitions;
        this.partitionStart = partOffset;
        this.numRuns = numRuns;

        this.queueSize = this.concurrency * 2;
        this.failures = metrics.meter(name(Reader.class, "failed"));
        this.reads = metrics.timer(name(Reader.class, "reads"));

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(this.concurrency * 2);

        metrics.register(name(Reader.class, "selects", "enqueued"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return queue.size();
            }
        });

        metrics.register(name(Reader.class, "runs"), new Gauge<Integer>() {
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
    }

    public void execute() {
        PreparedStatement prepared = this.session.prepare(QUERY);

        for (int i = 0; i < this.numRuns; i++) {
            for (int j = this.partitionStart; j < (this.numPartitions + this.partitionStart); j++) {
                final String key = Writer.keyName(j);
                final Timer.Context context = this.reads.time();
                executor.submit(() -> {
                    Statement statement = null;
                    try {
                        statement = prepared.bind(key).setConsistencyLevel(CL);
                        this.session.execute(statement);
                    }
                    catch (NoHostAvailableException | QueryExecutionException e) {
                        LOG.warn(e.getMessage());
                        this.failures.mark();
                    }
                    catch (Exception e) {
                        LOG.error("Unable to execute statement: \"{}\"", statement, e);
                        this.failures.mark();
                    }
                    finally {
                        context.stop();
                    }
                });
            }
            this.runCount++;
        }
        LOG.info("All jobs enqueued; Shutting down...");
        executor.shutdown();

        try {
            // Block until any jobs on the queue have completed, or the timeout
            // has expired.
            if (!executor.awaitTermination(Math.max(this.queueSize, 180), TimeUnit.SECONDS)) {
                LOG.warn("Timed out waiting for executor shutdown!");
            }
        }
        catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for executor shutdown", e);
        }
    }
}
