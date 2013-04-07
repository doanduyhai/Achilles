package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.List;

import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * ThriftLoaderImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftLoaderImpl
{
	private EntityMapper mapper = new EntityMapper();
	private EntityIntrospector introspector = new EntityIntrospector();

	@SuppressWarnings("unchecked")
	public <T, ID> T load(PersistenceContext<ID> context) throws Exception
	{
		Class<T> entityClass = (Class<T>) context.getEntityClass();
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		ID primaryKey = context.getPrimaryKey();

		List<Pair<DynamicComposite, String>> columns = context.getEntityDao().eagerFetchEntity(
				primaryKey);
		T entity = null;
		if (columns.size() > 0)
		{
			entity = entityClass.newInstance();
			mapper.setEagerPropertiesToEntity(primaryKey, columns, entityMeta, entity);
			introspector.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
		}

		return entity;
	}
}
