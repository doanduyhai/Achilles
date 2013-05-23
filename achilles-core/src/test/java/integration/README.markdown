## Integration tests

 The integration tests use an embedded Cassandra server: [Cassandra Unit][cassandraUnit]
 
 To deactivate the embedded server and run the tests with a stand-alone Cassandra server, add the following VM argument:
 
> -DcassandraHost=&lt;host&gt;:&lt;port&gt;

 Please note that you should create first an "achilles" keyspace in your Cassandra server as follow:
 
 * CREATE KEYSPACE achilles WITH placement_strategy = 'SimpleStrategy' AND strategy_options = {replication_factor:1}
 
[cassandraUnit]: https://github.com/jsevellec/cassandra-unit
 