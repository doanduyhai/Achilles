INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date1}, ['ONE'], {1.0}, {1: 'one'}, 'id - date1');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date2}, ['TWO'], {2.0}, {2: 'two'}, 'id - date2');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date3}, ['THREE'], {3.0}, {3: 'three'}, 'id - date3');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date4}, ['QUORUM'], {4.0}, {4: 'four'}, 'id - date4');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date5}, ['ALL'], {5.0}, {5: 'five'}, 'id - date5');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date6}, ['LOCAL_ONE'], {6.0}, {6: 'six'}, 'id - date6');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date7}, ['LOCAL_QUORUM'], {7.0}, {7: 'seven'}, 'id - date7');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date8}, ['EACH_QUORUM'], {8.0}, {8: 'eight'}, 'id - date8');

INSERT INTO achilles_embedded.simple(id, date, consistencylist, simpleset, simplemap, value)
VALUES(${id}, ${date9}, ['SERIAL'], {9.0}, {9: 'nine'}, 'id - date9');