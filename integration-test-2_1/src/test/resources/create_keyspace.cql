CREATE KEYSPACE IF NOT EXISTS my_static_keyspace WITH
REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=false;