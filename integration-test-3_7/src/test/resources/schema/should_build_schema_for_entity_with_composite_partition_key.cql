CREATE TABLE IF NOT EXISTS my_ks.entity_composite_pk(
		id bigint,
		uuid uuid,
		value text,
		PRIMARY KEY((id, uuid)));