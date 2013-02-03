## Integration tests

 The integration tests use an embedded Cassandra server: [Cassandra Unit][https://github.com/jsevellec/cassandra-unit]
 
 To deactivate the embedded server and run the tests with a stand-alone Cassandra server, add the following VM argument:
 
> -DcassandraHost=<host>:<port>


 