package info.archinnov.achilles.entity.metadata.builder;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * EntityMetaTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaTestBuilder
{

	private EntityParser parser;

	public EntityMetaTestBuilder() {
		final ObjectMapper mapper = new ObjectMapper();
		ObjectMapperFactory factory = new ObjectMapperFactory()
		{

			@Override
			public <T> ObjectMapper getMapper(Class<T> type)
			{
				return mapper;
			}
		};

		parser = new EntityParser(factory);
	}

	public static EntityMetaTestBuilder entityMeta()
	{
		return new EntityMetaTestBuilder();
	}

	@SuppressWarnings("unchecked")
	public <ID extends Serializable> EntityMeta<ID> build(Keyspace keyspace,
			GenericDynamicCompositeDao<ID> dao, Class<?> entityClass,
			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetasToBeFilled)
	{

		if (joinPropertyMetasToBeFilled == null)
		{
			joinPropertyMetasToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
		}
		EntityMeta<ID> entityMeta = (EntityMeta<ID>) parser.parseEntity(keyspace, entityClass,
				joinPropertyMetasToBeFilled);
		entityMeta.setEntityDao(dao);

		return entityMeta;
	}
}
