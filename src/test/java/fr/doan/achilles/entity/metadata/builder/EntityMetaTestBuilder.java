package fr.doan.achilles.entity.metadata.builder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.parser.EntityParser;

public class EntityMetaTestBuilder
{

	private EntityParser parser = new EntityParser();

	public static EntityMetaTestBuilder entityMeta()
	{
		return new EntityMetaTestBuilder();
	}

	@SuppressWarnings("unchecked")
	public <ID extends Serializable> EntityMeta<ID> build(Keyspace keyspace, GenericEntityDao<ID> dao,
			Class<?> entityClass, ColumnFamilyHelper columnFamilyHelper,
			boolean forceColumnFamilyCreation)
	{

		Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		EntityMeta<ID> entityMeta = (EntityMeta<ID>) parser.parseEntity(keyspace, entityClass,
				entityMetaMap, columnFamilyHelper, forceColumnFamilyCreation);
		entityMeta.setEntityDao(dao);

		return entityMeta;
	}
}
