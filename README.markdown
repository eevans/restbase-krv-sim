restbase-krv-simulator
======================

Read/write data in Cassandra using an approximation of the RESTBase data-model.

Build
-----
    $ mvn package

Setup
-----
    $ cqlsh -f src/main/resources/schema.cql

Run
---
    $ java -jar target/restbase-krv-sim-{version}-full.jar help
    $ # Or alternately
    $ java -cp target/restbase-krv-sim-1.0.0-SNAPSHOT-full.jar org.wikimedia.cassandra.ReRenderer --help
