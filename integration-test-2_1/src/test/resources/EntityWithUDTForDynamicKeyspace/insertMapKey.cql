INSERT INTO dynamic_ks.dynamic_table(id, clust, udtmapkey)
VALUES(${id},{id: ${id}, "VALUE": 'value'},{{id: ${id}, "VALUE": 'value1'}: '1', {id: ${id}, "VALUE": 'value2'}: '2'});