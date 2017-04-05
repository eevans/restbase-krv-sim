package org.wikimedia.cassandra;

import java.io.IOException;

public class Schema {

    public String get() throws IOException {
        return new String(Util.bytes(Schema.class.getResourceAsStream("/schema.cql")));
    }

}
