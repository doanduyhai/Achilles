package info.archinnov.achilles.configuration;

/**
 * CQLConfigurationParameters
 * 
 * @author DuyHai DOAN
 * 
 */
public interface CQLConfigurationParameters
{
    String CONNECTION_CONTACT_POINTS_PARAM = "achilles.cassandra.connection.contactPoints";
    String CONNECTION_PORT_PARAM = "achilles.cassandra.connection.port";
    String KEYSPACE_NAME_PARAM = "achilles.cassandra.keyspace.name";
    String COMPRESSION_TYPE = "achilles.cassandra.compression.type";
    String RETRY_POLICY = "achilles.cassandra.retry.policy";
    String LOAD_BALANCING_POLICY = "achilles.cassandra.load.balancing.policy";
    String RECONNECTION_POLICY = "achilles.cassandra.reconnection.policy";
    String USERNAME = "achilles.cassandra.username";
    String PASSWORD = "achilles.cassandra.password";
    String DISABLE_JMX = "achilles.cassandra.disable.jmx";
    String DISABLE_METRICS = "achilles.cassandra.disable.metrics";
    String SSL_ENABLED = "achilles.cassandra.ssl.enabled";
    String SSL_OPTIONS = "achilles.cassandra.ssl.options";
}
