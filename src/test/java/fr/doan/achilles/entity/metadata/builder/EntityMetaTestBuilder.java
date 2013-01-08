package fr.doan.achilles.entity.metadata.builder;

import java.io.Serializable;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.parser.EntityParser;

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
