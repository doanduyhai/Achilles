CREATE TABLE IF NOT EXISTS my_ks.entity_with_clusterings(
		id bigint,
		uuid uuid,
		date timestamp,
		value text,
		PRIMARY KEY(id, uuid, date))
	WITH CLUSTERING ORDER BY(uuid ASC, date DESC);