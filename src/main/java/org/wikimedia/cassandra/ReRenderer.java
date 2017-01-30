package org.wikimedia.cassandra;

import static org.wikimedia.cassandra.CassandraSession.KEYSPACE;
import static org.wikimedia.cassandra.CassandraSession.TABLE;

import java.nio.ByteBuffer;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;

@Command(name = "rerender", description = "Simulate revision rerenders")
public class ReRenderer {
    private static final Logger LOG = LoggerFactory.getLogger(ReRenderer.class);

    private static final String INSERT = String
            .format("INSERT INTO %s.%s (key,rev,tid,value) VALUES (?,?,?,?)", KEYSPACE, TABLE);;
    private static final String SELECT = String.format(
            "SELECT TTL(value) as expiry,key,rev,tid,value FROM %s.%s WHERE key=? AND rev=? AND tid < ? LIMIT 5",
            KEYSPACE,
            TABLE);
    private static final String UPDATE = String
            .format("UPDATE %s.%s USING TTL ? SET value=? WHERE key=? AND rev=? AND tid=?", KEYSPACE, TABLE);

    @Option(name = { "-c", "--cassandra-contact-point" }, description = "Cassandra node contact")
    private String cassandraContact = "localhost";
    @Option(name = { "-n", "--num-renders" }, description = "Number of renders to create")
    private int numRenders = 10;
    @Option(name = { "-vs", "--value-size-mb" }, description = "Value size in megabytes")
    private int valueSizeMB = 5;
    @Option(name = { "-t", "--time-to-live" }, description = "Column TTL to apply to updated records")
    private int timeToLive = 300;
    @Inject
    private HelpOption<ReRenderer> help;

    private void execute() throws Exception {
        if (this.help.showHelpIfRequested()) {
            return;
        }

        String key = Writer.keyName(Integer.MAX_VALUE);
        int rev = Integer.MAX_VALUE;
        byte[] value = Util.randomBytes(this.valueSizeMB * 1024 * 1024);

        try (CassandraSession session = new CassandraSession(this.cassandraContact)) {
            PreparedStatement insertStatement = session.prepare(INSERT);
            PreparedStatement selectStatement = session.prepare(SELECT);
            PreparedStatement updateStatement = session.prepare(UPDATE);

            Statement query = null;
            UUID tid = null;
            for (int i = 0; i < this.numRenders; i++) {
                LOG.debug("Inserting 20MB value into key={}, rev={}", key, rev);
                tid = UUIDs.timeBased();
                query = insertStatement.bind(key, rev, tid, ByteBuffer.wrap(value));
                session.execute(query);
                LOG.debug("Inserted as tid={}", tid);

                LOG.debug("Updating...");
                int count = 0;
                query = selectStatement.bind(key, rev, tid);
                for (Row row : session.execute(query)) {
                    count += 1;
                    LOG.debug("Updating entry for TID={}", row.getUUID("tid"));
                    session.execute(updateStatement.bind(300, row.getBytes("value"), key, rev, tid));
                }
                LOG.debug("Update processed {} records", count);
            }
        }
    }

    public static void main(String... args) throws Exception {
        SingleCommand<ReRenderer> parser = SingleCommand.singleCommand(ReRenderer.class);
        ReRenderer command = parser.parse(args);
        command.execute();
    }

}
