CREATE KEYSPACE IF NOT EXISTS dynamic_ks_json
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND DURABLE_WRITES = false;

CREATE TYPE IF NOT EXISTS dynamic_ks_json.no_keyspace_udt (id bigint, value text);

CREATE TABLE IF NOT EXISTS dynamic_ks_json.dynamic_table_json(
    id bigint,
    clust frozen<no_keyspace_udt>,
    udt frozen<no_keyspace_udt>,
    PRIMARY KEY((id), clust)
);

