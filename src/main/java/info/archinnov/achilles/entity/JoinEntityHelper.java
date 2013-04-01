package info.archinnov.achilles.entity;

import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * JoinEntityHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinEntityHelper
{

	private EntityMapper mapper = new EntityMapper();
	private EntityIntrospector introspector = new EntityIntrospector();

	public <T, ID> Map<ID, T> loadJoinEntities(Class<T> entityClass, List<ID> keys,
			EntityMeta<ID> entityMeta, GenericEntityDao<ID> joinEntityDao)
	{
		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotEmpty(keys, "List of join primary keys '" + keys
				+ "' should not be empty");
		Validator.validateNotNull(entityMeta, "Entity meta for '" + entityClass.getCanonicalName()
				+ "' should not be null");

		Map<ID, T> entitiesByKey = new HashMap<ID, T>();
		Map<ID, List<Pair<DynamicComposite, String>>> rows = joinEntityDao.eagerFetchEntities(keys);

		for (Entry<ID, List<Pair<DynamicComposite, String>>> entry : rows.entrySet())
		{
			T entity;
			try
			{
				entity = entityClass.newInstance();

				ID key = entry.getKey();
				List<Pair<DynamicComposite, String>> columns = entry.getValue();
				if (columns.size() > 0)
				{
					mapper.setEagerPropertiesToEntity(key, columns, entityMeta, entity);
					introspector.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);
					entitiesByKey.put(key, entity);
				}
			}
			catch (Exception e)
			{
				throw new AchillesException("Error when instantiating class '"
						+ entityClass.getCanonicalName() + "' ", e);
			}
		}
		return entitiesByKey;
	}
}
