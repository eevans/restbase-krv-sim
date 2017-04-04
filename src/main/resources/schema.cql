
/* -*- mode: sql; -*- */

CREATE KEYSPACE IF NOT EXISTS krv
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };

CREATE TABLE krv.data (
  key text,
  rev int,
  tid timeuuid,
  value blob,
  PRIMARY KEY(key, rev))
WITH CLUSTERING ORDER BY (rev DESC)
  AND compression = { 'chunk_length_kb': '256', 'sstable_compression': 'org.apache.cassandra.io.compress.DeflateCompressor' }
  AND gc_grace_seconds = 300;
  /* AND compaction = {} */
