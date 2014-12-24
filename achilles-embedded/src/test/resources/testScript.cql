CREATE TABLE IF NOT EXISTS test(
    id int PRIMARY KEY,
    value text
);

INSERT INTO test(id,value) VALUES(1,'test');

DELETE FROM test WHERE id=1;