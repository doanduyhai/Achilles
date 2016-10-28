INSERT INTO dynamic_ks.dynamic_table(id, clust, udtmapvalue)
VALUES(${id},{id: ${id}, "VALUE": 'value'},{1: {id: ${id}, "VALUE": 'value1'}, 2: {id: ${id}, "VALUE": 'value2'}});