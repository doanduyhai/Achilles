package info.archinnov.achilles.configuration;


/**
 * ConfigurationParameters
 * 
 * @author DuyHai DOAN
 * 
 */
public interface ThriftConfigurationParameters extends AchillesConfigurationParameters
{

	String HOSTNAME_PARAM = "achilles.cassandra.host";
	String CLUSTER_NAME_PARAM = "achilles.cassandra.cluster.name";
	String KEYSPACE_NAME_PARAM = "achilles.cassandra.keyspace.name";

	String CLUSTER_PARAM = "achilles.cassandra.cluster";
	String KEYSPACE_PARAM = "achilles.cassandra.keyspace";

}
