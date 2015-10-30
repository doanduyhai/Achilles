CREATE TABLE IF NOT EXISTS my_ks.simple(
		id bigint,
		date timestamp,
		consistencylist list<text>,
		simplemap map<int, text>,
		simpleset set<double>,
		value text,
		PRIMARY KEY(id, date))
	WITH CLUSTERING ORDER BY(date ASC);