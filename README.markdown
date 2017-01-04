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
