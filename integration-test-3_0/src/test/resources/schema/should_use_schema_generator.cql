CREATE TYPE IF NOT EXISTS test.layer2(
		layer text,
		layer3 frozen<test.layer3>);
CREATE TYPE IF NOT EXISTS test.layer3(
		layer text);
CREATE TYPE IF NOT EXISTS test.my_type(
		name text,
		list list<text>,
		map map<text, text>);
CREATE TYPE IF NOT EXISTS test.having_nested_type(
		value text,
		udtlist list<frozen<test.no_ks>>,
		nestedudt frozen<test.no_ks>,
		tuplewithudt frozen<tuple<int, no_ks>>);
CREATE TYPE IF NOT EXISTS test.no_ks(
		id bigint,
		"VALUE" text);
CREATE TABLE IF NOT EXISTS test.entity_child(
		id bigint,
		child_value text,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_for_json_function_call(
		id bigint,
		clust bigint,
		liststring list<text>,
		mapstring map<int, text>,
		setstring set<text>,
		value text,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);
CREATE TABLE IF NOT EXISTS test.layer(
		layer text,
		layer2 frozen<test.layer2>,
		PRIMARY KEY(layer));
CREATE TABLE IF NOT EXISTS test.sensor(
		sensor_id bigint,
		date bigint,
		type text,
		value double,
		PRIMARY KEY(sensor_id, date))
	WITH CLUSTERING ORDER BY(date ASC);
CREATE TABLE IF NOT EXISTS test.bean_validation(
		id bigint,
		list list<text>,
		udt frozen<achilles_embedded.my_type>,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.table_with_bytebuffer(
		id bigint,
		value blob,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_with_case_sensitive_pk(
		"partitionKey" bigint,
		"clusteringColumn" bigint,
		"listString" list<text>,
		"mapIntString" map<int, text>,
		"setString" set<text>,
		"udtWithNoKeyspace" frozen<test.no_ks>,
		PRIMARY KEY("partitionKey", "clusteringColumn"))
	WITH CLUSTERING ORDER BY("clusteringColumn" ASC);
CREATE TABLE IF NOT EXISTS test.entity_with_clusterings(
		id bigint,
		uuid uuid,
		date timestamp,
		value text,
		PRIMARY KEY(id, uuid, date))
	WITH CLUSTERING ORDER BY(uuid ASC, date DESC);
CREATE TABLE IF NOT EXISTS test.entity_complex_counters(
		id bigint,
		uuid uuid,
		static_count counter static,
		codec_count counter,
		count counter,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);
CREATE TABLE IF NOT EXISTS test.entitywithcomplexindices(
		id bigint,
		collectionindex list<text>,
		fullindexoncollection frozen<set<text>>,
		indexonmapentry map<int, text>,
		indexonmapkey map<text, text>,
		simpleindex text,
		PRIMARY KEY(id));


CREATE  INDEX IF NOT EXISTS collectionindex_index ON test.entitywithcomplexindices ( collectionindex );


CREATE  INDEX IF NOT EXISTS fullindexoncollection_index ON test.entitywithcomplexindices ( FULL(fullindexoncollection) );


CREATE  INDEX IF NOT EXISTS indexonmapentry_index ON test.entitywithcomplexindices ( ENTRIES(indexonmapentry) );


CREATE  INDEX IF NOT EXISTS indexonmapkey_index ON test.entitywithcomplexindices ( KEYS(indexonmapkey) );


CREATE  INDEX IF NOT EXISTS simpleindex_index ON test.entitywithcomplexindices ( simpleindex );
CREATE TABLE IF NOT EXISTS test.complex_tuple(
		id bigint,
		tuple frozen<tuple<int, map<int, list<text>>>>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_complex_types(
		id bigint,
		codec_on_class text,
		complex_nesting_map map<text, frozen<map<int, frozen<tuple<text, int, int>>>>>,
		consistencylevel text,
		doublearray frozen<list<double>>,
		encoding int,
		floatarray frozen<list<float>>,
		intarray frozen<list<int>>,
		integer text,
		intwrapper int,
		jdkinstant timestamp,
		jdklocaldate date,
		jdklocaltime time,
		jdkzoneddatetime frozen<tuple<timestamp, varchar>>,
		json_map text,
		list_nesting list<frozen<map<int, text>>>,
		list_udt list<frozen<achilles_embedded.my_type>>,
		listoflongarray list<frozen<list<bigint>>>,
		listofoptional list<text>,
		longarray frozen<list<bigint>>,
		map_udt map<int, frozen<achilles_embedded.my_type>>,
		map_with_nested_json map<int, text>,
		object_bool boolean,
		object_byte tinyint,
		object_byte_array blob,
		ok_set set<int>,
		optionalencodingasordinal int,
		optionalprotocolversion text,
		optionalstring text,
		primitive_bool boolean,
		primitive_byte tinyint,
		primitive_byte_array blob,
		protocolversion text,
		protocolversionasordinal int,
		simple_udt frozen<achilles_embedded.my_type>,
		time text,
		timeuuid timeuuid,
		tuple1 frozen<tuple<text>>,
		tuple2 frozen<tuple<text, text>>,
		tuple_nesting frozen<tuple<int, list<text>>>,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_composite_pk(
		id bigint,
		uuid uuid,
		value text,
		PRIMARY KEY((id, uuid)));
CREATE TABLE IF NOT EXISTS test.entity_counter(
		id bigint,
		count counter,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entitywithimplicitfieldparsing(
		id bigint,
		uuid uuid,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);
CREATE TABLE IF NOT EXISTS test.table_with_nested_udt(
		id bigint,
		complexudt frozen<test.having_nested_type>,
		udt frozen<test.no_ks>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS non_existing.test_table(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.non_existing_table(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entitywithsimplepartitionkey(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS my_static_keyspace.entity_static_annotations(
		partition_key bigint,
		"overRiden" text,
		value text,
		PRIMARY KEY(partition_key))
	WITH default_time_to_live = 1;
CREATE TABLE IF NOT EXISTS test.entitywithstaticcolumn(
		id bigint,
		uuid uuid,
		another_static_col text static,
		static_col text static,
		value text,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);
CREATE TABLE IF NOT EXISTS test.entity_static_counter(
		id bigint,
		uuid uuid,
		static_count counter static,
		count counter,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);
CREATE TABLE IF NOT EXISTS test.simple(
		id bigint,
		date timestamp,
		consistencylist list<text>,
		simplemap map<int, text>,
		simpleset set<double>,
		value text,
		PRIMARY KEY(id, date))
	WITH CLUSTERING ORDER BY(date ASC);