CREATE TABLE IF NOT EXISTS my_ks.entity_static_counter(
		id bigint,
		uuid uuid,
		static_count counter static,
		count counter,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);