# Schema creation in DSE

```sql
CREATE KEYSPACE IF NOT EXISTS achilles_dse_it WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS achilles_dse_it.search(
    id bigint PRIMARY KEY,
    string text,
    numeric float,
    date timestamp
);
```

# Solr core creation in DSE

```sh

${DSE_HOME}/bin/dsetool create_core achilles_dse_it.search generateResources=true reindex=true
```