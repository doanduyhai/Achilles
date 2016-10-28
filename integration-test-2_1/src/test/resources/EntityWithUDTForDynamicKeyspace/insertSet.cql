INSERT INTO dynamic_ks.dynamic_table(id, clust, udtset)
VALUES(${id},{id: ${id}, "VALUE": 'value'},{{id: ${id}, "VALUE": 'value1'}, {id: ${id}, "VALUE": 'value2'}});