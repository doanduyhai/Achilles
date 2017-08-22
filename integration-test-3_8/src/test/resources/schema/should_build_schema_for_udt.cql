CREATE TYPE IF NOT EXISTS my_ks.my_type(
		name text,
		list list<text>,
		map map<text, text>);