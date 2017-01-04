package org.wikimedia.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class CassandraSession implements AutoCloseable {
    public static final String KEYSPACE = "krv_simulation";
    public static final String TABLE = "data";

    private final Cluster cluster;

    private Session session;

    public CassandraSession(String...contacts) {
        this.cluster = Cluster.builder().addContactPoints(contacts).build();
        this.session = this.cluster.connect();
    }

    public ResultSet execute(Statement statement) {
        return this.session.execute(statement);
    }

    public PreparedStatement prepare(String query) {
        return this.session.prepare(query);
    }

    @Override
    public void close() throws Exception {
        cluster.close();
    }

}
