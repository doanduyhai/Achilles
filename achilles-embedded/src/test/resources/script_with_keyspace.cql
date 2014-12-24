CREATE KEYSPACE IF NOT EXISTS my_ks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES=false;
CREATE TABLE IF NOT EXISTS my_ks.my_table(
    key int PRIMARY KEY,
    value text);

INSERT INTO  my_ks.my_table(key,value) VALUES(1,'one');
INSERT INTO  my_ks.my_table(key,value) VALUES(2,'two');
INSERT INTO  my_ks.my_table(key,value) VALUES(3,'three');