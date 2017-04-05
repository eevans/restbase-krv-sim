
/* -*- mode: sql; -*- */

CREATE KEYSPACE IF NOT EXISTS alt_write
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };

CREATE TABLE alt_write.data (
  key text PRIMARY KEY,
  value blob)
WITH
  compression = { 'chunk_length_kb': '256', 'sstable_compression': 'org.apache.cassandra.io.compress.DeflateCompressor' }
  AND gc_grace_seconds = 300;
  /* AND compaction = {} */
