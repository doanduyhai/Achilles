CREATE TABLE IF NOT EXISTS my_ks.entitywithcomplexindices(
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


CREATE  INDEX IF NOT EXISTS collectionindex_index ON my_ks.entitywithcomplexindices ( collectionindex );


CREATE  INDEX IF NOT EXISTS fullindexoncollection_index ON my_ks.entitywithcomplexindices ( FULL(fullindexoncollection) );


CREATE  INDEX IF NOT EXISTS indexonmapentry_index ON my_ks.entitywithcomplexindices ( ENTRIES(indexonmapentry) );


CREATE  INDEX IF NOT EXISTS indexonmapkey_index ON my_ks.entitywithcomplexindices ( KEYS(indexonmapkey) );


CREATE  INDEX IF NOT EXISTS indexonmapvalue_index ON my_ks.entitywithcomplexindices ( indexonmapvalue );


CREATE  INDEX IF NOT EXISTS simpleindex_index ON my_ks.entitywithcomplexindices ( simpleindex );