CREATE KEYSPACE IF NOT EXISTS missing_schema
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=false;

CREATE TABLE IF NOT EXISTS missing_schema.missing_partition(
    id bigint,
    bucket int,
    value text,
    PRIMARY KEY((id, bucket))
);