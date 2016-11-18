CREATE KEYSPACE IF NOT EXISTS new_ks WITH
REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=false;

CREATE KEYSPACE IF NOT EXISTS overriden_schema_name WITH
REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=false;

CREATE TYPE IF NOT EXISTS overriden_schema_name.udt_no_ks(
    id bigint,
    value text);

CREATE TABLE IF NOT EXISTS overriden_schema_name.entitywithudts(
    id bigint,
    listudt list<frozen<udt_no_ks>>,
    mapudt map<frozen<udt_no_ks>, frozen<udt_no_ks>>,
    optionaludt udt_no_ks,
    setudt set<frozen<udt_no_ks>>,
    tupleudt frozen<tuple<int, udt_no_ks>>,
    PRIMARY KEY(id));