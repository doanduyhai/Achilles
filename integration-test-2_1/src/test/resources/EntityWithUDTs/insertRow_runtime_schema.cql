INSERT INTO overriden_schema_name.entitywithudts(id,listudt,mapudt,optionaludt,setudt,tupleudt)
VALUES(${id}, [{id: ${id}, value: 'val'}],
{{id: ${id}, value: 'val'}: {id: ${id}, value: 'val'}},
{id: ${id}, value: 'val'},
{{id: ${id}, value: 'val'}},
(1, {id: ${id}, value: 'val'}));