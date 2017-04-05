package org.wikimedia.cassandra;

import static org.wikimedia.cassandra.AltWriter.KEYSPACE;
import static org.wikimedia.cassandra.CassandraSession.TABLE;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.github.rvesse.airline.annotations.Command;

@Command(name = "alt-read", description = "Read data")
public class AltReader extends Reader {

    private static final String QUERY = String
            .format("SELECT value FROM %s.%s WHERE key=?", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(AltReader.class);

    public AltReader(
            MetricRegistry metrics,
            CassandraSession session,
            int concurrency,
            int numPartitions,
            int partOffset) {
        super(metrics, session, concurrency, numPartitions, partOffset, -1, -1);
    }

    public void execute() {
        PreparedStatement prepared = this.session.prepare(QUERY);

        for (int i = this.partitionStart; i < (this.numPartitions + this.partitionStart); i++) {
            final String key = Writer.keyName(i);
            final Timer.Context context = this.reads.time();
            executor.submit(() -> {
                Statement statement = null;
                try {
                    statement = prepared.bind(key);
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
