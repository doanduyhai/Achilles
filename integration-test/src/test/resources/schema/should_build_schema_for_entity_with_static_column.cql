CREATE TABLE IF NOT EXISTS my_ks.entitywithstaticcolumn(
		id bigint,
		uuid uuid,
		static_col text static,
		value text,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);