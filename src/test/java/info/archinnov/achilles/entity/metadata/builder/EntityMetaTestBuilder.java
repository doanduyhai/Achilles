package info.archinnov.achilles.entity.metadata.builder;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityParser;

import java.io.Serializable;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;

/**
 * EntityMetaTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaTestBuilder
{

	private EntityParser parser = new EntityParser();

	public static EntityMetaTestBuilder entityMeta()
	{
		return new EntityMetaTestBuilder();
	}

	@SuppressWarnings("unchecked")
	public <ID extends Serializable> EntityMeta<ID> build(Keyspace keyspace,
			GenericDynamicCompositeDao<ID> dao, Class<?> entityClass,
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetasToBeFilled)
	{

		EntityMeta<ID> entityMeta = (EntityMeta<ID>) parser.parseEntity(keyspace, entityClass,
				joinPropertyMetasToBeFilled);
		entityMeta.setEntityDao(dao);

		return entityMeta;
	}
}
