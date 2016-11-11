CREATE KEYSPACE IF NOT EXISTS missing_schema
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=false;

CREATE TABLE IF NOT EXISTS missing_schema.missing_clustering(
    id bigint,
    clust uuid,
    missing_clust int,
    value text,
    PRIMARY KEY((id), clust, missing_clust)
);