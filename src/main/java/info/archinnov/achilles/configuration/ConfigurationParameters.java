package info.archinnov.achilles.configuration;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

/**
 * ConfigurationParameters
 * 
 * @author DuyHai DOAN
 * 
 */
public interface ConfigurationParameters
{
	String ENTITY_PACKAGES_PARAM = "achilles.entity.packages";

	String HOSTNAME_PARAM = "achilles.cassandra.host";
	String CLUSTER_NAME_PARAM = "achilles.cassandra.cluster.name";
	String KEYSPACE_NAME_PARAM = "achilles.cassandra.keyspace.name";

	String CLUSTER_PARAM = "achilles.cassandra.cluster";
	String KEYSPACE_PARAM = "achilles.cassandra.keyspace";

	String OBJECT_MAPPER_FACTORY_PARAM = "achilles.json.object.mapper.factory";
	String OBJECT_MAPPER_PARAM = "achilles.json.object.mapper";

	String DEFAUT_READ_CONSISTENCY_PARAM = "achilles.default.consistency.read";
	String DEFAUT_WRITE_CONSISTENCY_PARAM = "achilles.default.consistency.write";
	String READ_CONSISTENCY_MAP_PARAM = "achilles.consistency.read.map";
	String WRITE_CONSISTENCY_MAP_PARAM = "achilles.consistency.write.map";

	String FORCE_CF_CREATION_PARAM = "achilles.ddl.force.column.family.creation";
	String ENSURE_CONSISTENCY_ON_JOIN_PARAM = "achilles.consistency.join.check";

	ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.ONE;
}
