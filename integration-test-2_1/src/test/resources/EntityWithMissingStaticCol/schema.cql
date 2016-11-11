CREATE KEYSPACE IF NOT EXISTS missing_schema
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=false;

CREATE TABLE IF NOT EXISTS missing_schema.missing_static(
    id bigint,
    staticcol text static,
    missing_static int static,
    clust uuid,
    value text,
    PRIMARY KEY((id), clust)
);