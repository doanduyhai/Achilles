INSERT INTO new_ks.entity_with_udts(id,listudt,mapudt,optionaludt,setudt,tupleudt)
VALUES(${id}, [{id: ${id}, value: 'val'}],
{{id: ${id}, value: 'val'}: {id: ${id}, value: 'val'}},
{id: ${id}, value: 'val'},
{{id: ${id}, value: 'val'}},
(1, {id: ${id}, value: 'val'}));