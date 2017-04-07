package org.wikimedia.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class CassandraSession implements AutoCloseable {
    public static final String KEYSPACE = "krv";
    public static final String TABLE = "data";

    private final Cluster cluster;

    private Session session;

    private CassandraSession(Cluster cluster) {
        this.cluster = cluster;
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
        this.cluster.close();
    }

    public static class Builder {
        private final Cluster.Builder builder;

        public Builder() {
            this.builder = Cluster.builder();
        }

        public Builder addContactPoints(String... contacts) {
            this.builder.addContactPoints(contacts);
            return this;
        }

        public Builder withSSL(boolean ssl) {
            if (ssl) {
                this.builder.withSSL();
            }
            return this;
        }

        public Builder withCredentials(String username, String password) {
            this.builder.withCredentials(username, password);
            return this;
        }

        public CassandraSession build() {
            return new CassandraSession(this.builder.build());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

}
