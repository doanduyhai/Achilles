package info.archinnov.achilles.configuration;

import info.archinnov.achilles.entity.type.ConsistencyLevel;

/**
 * AchillesConfigurationParameters
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesConfigurationParameters
{
	String ENTITY_PACKAGES_PARAM = "achilles.entity.packages";

	String OBJECT_MAPPER_FACTORY_PARAM = "achilles.json.object.mapper.factory";
	String OBJECT_MAPPER_PARAM = "achilles.json.object.mapper";

	String CONSISTENCY_LEVEL_READ_DEFAULT_PARAM = "achilles.consistency.read.default";
	String CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM = "achilles.consistency.write.default";
	String CONSISTENCY_LEVEL_READ_MAP_PARAM = "achilles.consistency.read.map";
	String CONSISTENCY_LEVEL_WRITE_MAP_PARAM = "achilles.consistency.write.map";

	String FORCE_CF_CREATION_PARAM = "achilles.ddl.force.column.family.creation";
	String ENSURE_CONSISTENCY_ON_JOIN_PARAM = "achilles.consistency.join.check";

	ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.ONE;
}
