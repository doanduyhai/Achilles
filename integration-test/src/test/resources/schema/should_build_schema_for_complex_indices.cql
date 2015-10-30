CREATE TABLE IF NOT EXISTS my_ks.entitywithcomplexindices(
		id bigint,
		collectionindex list<text>,
		fullindexoncollection frozen<set<text>>,
		indexonmapentry map<int, text>,
		indexonmapkey map<text, text>,
		simpleindex text,
		PRIMARY KEY(id));


CREATE  INDEX IF NOT EXISTS collectionindex_index ON my_ks.entitywithcomplexindices ( collectionindex );


CREATE  INDEX IF NOT EXISTS fullindexoncollection_index ON my_ks.entitywithcomplexindices ( FULL(fullindexoncollection) );


CREATE  INDEX IF NOT EXISTS indexonmapentry_index ON my_ks.entitywithcomplexindices ( ENTRIES(indexonmapentry) );


CREATE  INDEX IF NOT EXISTS indexonmapkey_index ON my_ks.entitywithcomplexindices ( KEYS(indexonmapkey) );


CREATE  INDEX IF NOT EXISTS simpleindex_index ON my_ks.entitywithcomplexindices ( simpleindex );