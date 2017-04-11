package org.wikimedia.cassandra;

import static org.wikimedia.cassandra.AltWriter.TABLE;
import static org.wikimedia.cassandra.CassandraSession.KEYSPACE;

import java.nio.ByteBuffer;

import org.wikimedia.cassandra.Util.AltValue;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class GetAltValue {

    private static final String QUERY = String.format("SELECT value FROM %s.%s WHERE key=?", KEYSPACE, TABLE);

    private final CassandraSession session;
    private final PreparedStatement prepared;

    public GetAltValue(CassandraSession session) {
        this.session = session;
        this.prepared = this.session.prepare(QUERY);
    }

    public AltValue get(String key) {
        ResultSet r = this.session.execute(this.prepared.bind(key));
        Row row = r.one();
        if (row == null)
            return null;
        ByteBuffer value = row.getBytes("value");
        return AltValue.create(value);
    }
}
