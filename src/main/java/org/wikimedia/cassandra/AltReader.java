package org.wikimedia.cassandra;

import static org.wikimedia.cassandra.CassandraSession.KEYSPACE;
import static org.wikimedia.cassandra.AltWriter.TABLE;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.github.rvesse.airline.annotations.Command;

@Command(name = "alt-read", description = "Read data")
public class AltReader extends Reader {
    private static final ConsistencyLevel CL = ConsistencyLevel.LOCAL_QUORUM;
    private static final String QUERY = String
            .format("SELECT value FROM %s.%s WHERE key=?", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(AltReader.class);

    public AltReader(
            MetricRegistry metrics,
            CassandraSession session,
            int concurrency,
            int numPartitions,
            int partOffset,
            int numRuns,
            double traceProbability) {
        super(metrics, session, concurrency, numPartitions, partOffset, numRuns, traceProbability);
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
                        boolean execTrace = Math.random() <= this.traceProbability;
                        if (execTrace) {
                            statement.enableTracing();
                        }
                        ResultSet result = this.session.execute(statement);
                        if (execTrace) {
                            ExecutionInfo info = result.getExecutionInfo();
                            Host coordinator = info.getQueriedHost();
                            UUID traceId = info.getQueryTrace().getTraceId();
                            LOG.info("Query trace: coordinatorNode={}, traceID={}", coordinator, traceId);
                        }
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
