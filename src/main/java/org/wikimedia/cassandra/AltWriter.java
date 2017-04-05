package org.wikimedia.cassandra;

import static org.wikimedia.cassandra.CassandraSession.TABLE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.utils.UUIDs;
import com.github.rvesse.airline.annotations.Command;

@Command(name = "alt-write", description = "Write data")
public class AltWriter extends Writer {
    private static final String KEYSPACE = "alt_write";
    private static final String QUERY = String
            .format("INSERT INTO %s.%s (key,value) VALUES (?,?) USING TIMESTAMP 1", KEYSPACE, TABLE);
    private static final Logger LOG = LoggerFactory.getLogger(AltWriter.class);

    public AltWriter(
            MetricRegistry metrics,
            CassandraSession session,
            int concurrency,
            int numPartitions,
            int partOffset,
            int numRevisions,
            int revOffset,
            int numRenders,
            int numRuns) throws IOException {
        super(metrics, session, concurrency, numPartitions, partOffset, numRevisions, revOffset, numRenders, numRuns);
    }

    public void execute() {
        PreparedStatement prepared = this.session.prepare(QUERY);

        for (int run = 0; run < this.numRuns; run++) {
            for (int i = this.revisionStart; i < (this.numRevisions + this.revisionStart); i++) {
                for (int j = 0; j < this.numRenders; j++) {
                    for (int k = this.partitionStart; k < (this.numPartitions + this.partitionStart); k++) {
                        final String key = keyName(k);
                        final int rev = i;
                        executor.submit(() -> {
                            Statement statement = null;
                            try {
                                statement = prepared.bind(key, valueFor(rev));
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

    ByteBuffer valueFor(int rev) {
        return valueFor(rev, this.value.duplicate());
    }

    static ByteBuffer valueFor(int rev, ByteBuffer value) {
        UUID uid = UUIDs.timeBased();
        ByteBuffer res = ByteBuffer.allocate(value.remaining() + 16 + 4);
        res.putInt(rev);
        res.putLong(uid.getMostSignificantBits());
        res.putLong(uid.getLeastSignificantBits());
        res.put(value);
        value.flip();
        res.flip();
        return res;
    }
}
