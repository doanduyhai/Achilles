INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, '2015-10-01 00:00:00+0000', ['QUORUM', 'LOCAL_ONE'], {1.0, 2.0}, {10: 'ten', 20: 'twenty'}, '0 AM');
INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, '2015-10-01 01:00:00+0000', ['QUORUM', 'LOCAL_ONE'], {1.0, 2.0}, {10: 'ten', 20: 'twenty'}, '1 AM');