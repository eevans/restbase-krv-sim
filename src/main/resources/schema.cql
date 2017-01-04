
/* -*- mode: sql; -*- */

CREATE KEYSPACE IF NOT EXISTS krv_simulation
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };

CREATE TABLE krv_simulation.data (
  key text,
  rev int,
  tid timeuuid,
  value blob,
  PRIMARY KEY(key, rev, tid))
WITH CLUSTERING ORDER BY (rev DESC, tid DESC)
  AND compression = { 'chunk_length_kb': '256', 'sstable_compression': 'org.apache.cassandra.io.compress.DeflateCompressor' }
  AND gc_grace_seconds = 300;
  /* AND compaction = {} */
