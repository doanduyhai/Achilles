CREATE TABLE IF NOT EXISTS my_ks.entity_complex_counters(
		id bigint,
		uuid uuid,
		static_count counter static,
		codec_count counter,
		count counter,
		PRIMARY KEY(id, uuid))
	WITH CLUSTERING ORDER BY(uuid ASC);