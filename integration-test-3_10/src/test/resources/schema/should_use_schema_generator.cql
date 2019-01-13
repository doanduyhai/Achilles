CREATE TYPE IF NOT EXISTS test.adress_udt(
		street text,
		number int);
CREATE TYPE IF NOT EXISTS test.CaseSensitiveUDT(
		"Quantity" int,
		value text);
CREATE TYPE IF NOT EXISTS test.immutable_udt(
		idx bigint,
		value text);
CREATE TYPE IF NOT EXISTS test.layer2(
		layer text,
		layer3 frozen<test.layer3>);
CREATE TYPE IF NOT EXISTS test.layer3(
		layer text);
CREATE TYPE IF NOT EXISTS test.no_keyspace_udt(
		id bigint,
		value text);
CREATE TYPE IF NOT EXISTS test.non_frozen_udt(
		val text,
		li frozen<list<text>>,
		se frozen<set<text>>,
		ma frozen<map<int, text>>,
		address frozen<it_3_6.adress_udt>);
CREATE TYPE IF NOT EXISTS test.udt_no_ks(
		id bigint,
		value text);
CREATE TYPE IF NOT EXISTS test.my_type(
		name text,
		list list<text>,
		map map<text, text>);
CREATE TYPE IF NOT EXISTS test.udt_collections_primitive(
		listint list<int>,
		setdouble set<double>);
CREATE TYPE IF NOT EXISTS test.custom_constructor_declared_fields_udt(
		idx bigint,
		value text);
CREATE TYPE IF NOT EXISTS test.custom_constructor_udt(
		idx bigint,
		value text);
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
CREATE TABLE IF NOT EXISTS it_3_8.entity_for_aggregate(
		partition bigint,
		clustering int,
		doubleval double,
		stringval text,
		PRIMARY KEY(partition, clustering))
	WITH CLUSTERING ORDER BY(clustering ASC);
CREATE TABLE IF NOT EXISTS test.entity_for_cast_fcall(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS it_3_10.entity_for_group_by(
		id bigint,
		"uuID" uuid,
		clust1 int,
		"clusteRing2" int,
		clust3 int,
		val int,
		PRIMARY KEY((id, "uuID"), clust1, "clusteRing2", clust3))
	WITH CLUSTERING ORDER BY(clust1 ASC, "clusteRing2" ASC, clust3 ASC);
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
		priority int,
		"listString" list<text>,
		"mapIntString" map<int, text>,
		"setString" set<text>,
		"udtWithNoKeyspace" frozen<test.no_ks>,
		PRIMARY KEY("partitionKey", "clusteringColumn", priority))
	WITH CLUSTERING ORDER BY("clusteringColumn" ASC, priority ASC);
CREATE TABLE IF NOT EXISTS "MyKeyspace".entity_with_case_sensitive_udt(
		id text,
		details frozen<"MyKeyspace"."CaseSensitiveUDT">,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_with_clusterings(
		id bigint,
		uuid uuid,
		date timestamp,
		value text,
		PRIMARY KEY(id, uuid, date))
	WITH CLUSTERING ORDER BY(uuid ASC, date DESC);
CREATE TABLE IF NOT EXISTS it_3_6.entity_with_clustering(
		id bigint,
		clust bigint,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);
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
		clust1 int,
		clust2 int,
		clust3 text,
		collectionindex list<text>,
		fullindexoncollection frozen<set<text>>,
		indexonmapentry map<int, text>,
		indexonmapkey map<text, text>,
		indexonmapvalue map<int, text>,
		simpleindex text,
		PRIMARY KEY(id, clust1, clust2, clust3))
	WITH CLUSTERING ORDER BY(clust1 ASC, clust2 ASC, clust3 ASC);


CREATE  INDEX IF NOT EXISTS collectionindex_index ON test.entitywithcomplexindices ( collectionindex );


CREATE  INDEX IF NOT EXISTS fullindexoncollection_index ON test.entitywithcomplexindices ( FULL(fullindexoncollection) );


CREATE  INDEX IF NOT EXISTS indexonmapentry_index ON test.entitywithcomplexindices ( ENTRIES(indexonmapentry) );


CREATE  INDEX IF NOT EXISTS indexonmapkey_index ON test.entitywithcomplexindices ( KEYS(indexonmapkey) );


CREATE  INDEX IF NOT EXISTS indexonmapvalue_index ON test.entitywithcomplexindices ( indexonmapvalue );


CREATE  INDEX IF NOT EXISTS simpleindex_index ON test.entitywithcomplexindices ( simpleindex );
CREATE TABLE IF NOT EXISTS test.complex_tuple(
		id bigint,
		tuple frozen<tuple<int, map<int, list<text>>>>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_complex_types(
		id bigint,
		ascii ascii,
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
CREATE TABLE IF NOT EXISTS test.custom_constructor_declared_fields(
		id bigint,
		name text,
		udt frozen<test.custom_constructor_declared_fields_udt>,
		value double,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_custom_constructor(
		id bigint,
		name text,
		udt frozen<test.custom_constructor_udt>,
		value double,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS it_3_10.entity_with_duration_type(
		id bigint,
		duration duration,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entitywithimplicitfieldparsing(
		id bigint,
		uuid uuid,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);
CREATE TABLE IF NOT EXISTS test.index_on_clustering(
		partition bigint,
		clustering int,
		value text,
		PRIMARY KEY(partition, clustering))
	WITH CLUSTERING ORDER BY(clustering ASC);


CREATE  INDEX IF NOT EXISTS clustering_index ON test.index_on_clustering ( clustering );
CREATE TABLE IF NOT EXISTS test.entity_with_indices_for_json(
		id bigint,
		clust1 int,
		clust2 int,
		clust3 text,
		collectionindex list<text>,
		fullindexoncollection frozen<set<text>>,
		indexonmapentry map<int, text>,
		indexonmapkey map<text, text>,
		indexonmapvalue map<int, text>,
		simpleindex text,
		PRIMARY KEY(id, clust1, clust2, clust3))
	WITH CLUSTERING ORDER BY(clust1 ASC, clust2 ASC, clust3 ASC);


CREATE  INDEX IF NOT EXISTS collectionindex_index ON test.entity_with_indices_for_json ( collectionindex );


CREATE  INDEX IF NOT EXISTS fullindexoncollection_index ON test.entity_with_indices_for_json ( FULL(fullindexoncollection) );


CREATE  INDEX IF NOT EXISTS indexonmapentry_index ON test.entity_with_indices_for_json ( ENTRIES(indexonmapentry) );


CREATE  INDEX IF NOT EXISTS indexonmapkey_index ON test.entity_with_indices_for_json ( KEYS(indexonmapkey) );


CREATE  INDEX IF NOT EXISTS indexonmapvalue_index ON test.entity_with_indices_for_json ( indexonmapvalue );


CREATE  INDEX IF NOT EXISTS simpleindex_index ON test.entity_with_indices_for_json ( simpleindex );
CREATE TABLE IF NOT EXISTS missing_schema.missing_clustering(
		id bigint,
		clust uuid,
		value text,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);
CREATE TABLE IF NOT EXISTS missing_schema.missing_partition(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS missing_schema.missing_static(
		id bigint,
		clust uuid,
		staticcol text static,
		value text,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);
CREATE TABLE IF NOT EXISTS test.entity_with_native_collections(
		id bigint,
		doublelist list<double>,
		longlist list<bigint>,
		mapintlong map<int, bigint>,
		tuple2 frozen<tuple<list<int>, list<double>>>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.table_with_nested_udt(
		id bigint,
		complexudt frozen<test.having_nested_type>,
		optionaludt test.no_ks,
		udt frozen<test.no_ks>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_with_no_ks_udt(
		id bigint,
		clust frozen<test.no_keyspace_udt>,
		udt frozen<test.no_keyspace_udt>,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);
CREATE TABLE IF NOT EXISTS non_existing.test_table(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.non_existing_table(
		id bigint,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS it_3_6.non_frozen_udt(
		id bigint,
		nonfrozen it_3_6.non_frozen_udt,
		value text,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.entity_primitive_clustering(
		partition bigint,
		clustering boolean,
		value text,
		PRIMARY KEY(partition, clustering))
	WITH CLUSTERING ORDER BY(clustering ASC);
CREATE TABLE IF NOT EXISTS it_3_7.entity_with_sasi_indices(
		id bigint,
		clust int,
		containsstandardanalyzer text,
		numeric int,
		prefixnontokenizer text,
		sparse int,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);


CREATE CUSTOM INDEX IF NOT EXISTS containsstandardanalyzer_index ON test.entity_with_sasi_indices ( containsstandardanalyzer )  USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'max_compaction_flush_memory_in_mb' : '1024','mode' : 'CONTAINS','analyzed' : 'true','analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer','tokenization_locale': 'en','tokenization_enable_stemming': 'true','tokenization_skip_stop_words': 'false','tokenization_normalize_lowercase': 'true'};


CREATE CUSTOM INDEX IF NOT EXISTS numeric_index ON test.entity_with_sasi_indices ( numeric )  USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'max_compaction_flush_memory_in_mb' : '1024','mode' : 'PREFIX'};


CREATE CUSTOM INDEX IF NOT EXISTS prefixnontokenizer_index ON test.entity_with_sasi_indices ( prefixnontokenizer )  USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'max_compaction_flush_memory_in_mb' : '1024','mode' : 'PREFIX','analyzed' : 'true','analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer','normalize_lowercase': 'true'};


CREATE CUSTOM INDEX IF NOT EXISTS sparse_index ON test.entity_with_sasi_indices ( sparse )  USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'max_compaction_flush_memory_in_mb' : '1024','mode' : 'SPARSE'};
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
CREATE TABLE IF NOT EXISTS test.entity_with_udt_collections_primitives(
		id bigint,
		udt frozen<test.udt_collections_primitive>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.table_with_with_dynamic_keyspace(
		id bigint,
		clust frozen<test.no_ks>,
		udt frozen<test.no_ks>,
		udtlist list<frozen<test.no_ks>>,
		udtmapkey map<frozen<test.no_ks>, text>,
		udtmapvalue map<int, frozen<test.no_ks>>,
		udtset set<frozen<test.no_ks>>,
		PRIMARY KEY(id, clust))
	WITH CLUSTERING ORDER BY(clust ASC);
CREATE TABLE IF NOT EXISTS test.entity_with_udts(
		id bigint,
		listudt list<frozen<test.udt_no_ks>>,
		mapudt map<frozen<test.udt_no_ks>, frozen<test.udt_no_ks>>,
		optionaludt test.udt_no_ks,
		setudt set<frozen<test.udt_no_ks>>,
		tupleudt frozen<tuple<int, udt_no_ks>>,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.immutable_entity(
		id bigint,
		name text,
		udt frozen<test.immutable_udt>,
		value double,
		PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS test.multi_clustering_entity(
		id bigint,
		c1 int,
		c2 int,
		value text,
		PRIMARY KEY(id, c1, c2))
	WITH CLUSTERING ORDER BY(c1 ASC, c2 ASC);
CREATE TABLE IF NOT EXISTS test.simple(
		id bigint,
		date timestamp,
		consistencylist list<text>,
		simplemap map<int, text>,
		simpleset set<double>,
		value text,
		PRIMARY KEY(id, date))
	WITH CLUSTERING ORDER BY(date ASC);