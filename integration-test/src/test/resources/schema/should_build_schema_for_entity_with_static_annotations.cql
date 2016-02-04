CREATE TABLE IF NOT EXISTS my_static_keyspace.entity_static_annotations(
		partition_key bigint,
		"overRiden" text,
		value text,
		PRIMARY KEY(partition_key))
	WITH default_time_to_live = 1;