CREATE KEYSPACE IF NOT EXISTS dynamic_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND DURABLE_WRITES = false;

CREATE TYPE IF NOT EXISTS dynamic_ks.no_ks (id bigint, "VALUE" text);

CREATE TABLE IF NOT EXISTS dynamic_ks.dynamic_table(
    id bigint,
    clust frozen<no_ks>,
    udt frozen<no_ks>,
    udtlist list<frozen<no_ks>>,
    udtset set<frozen<no_ks>>,
    udtmapkey map<frozen<no_ks>, text>,
    udtmapvalue map<int, frozen<no_ks>>,
    PRIMARY KEY((id), clust)
);

