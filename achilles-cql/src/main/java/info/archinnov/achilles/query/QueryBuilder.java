package info.archinnov.achilles.query;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

/**
 * QueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class QueryBuilder
{

	private Map<Class<?>, EntityMeta> entityMetaMap;
	private ConfigurationContext configContext;
	private CQLDaoContext daoContext;

	public QueryBuilder(Map<Class<?>, EntityMeta> entityMetaMap,
			ConfigurationContext configContext, CQLDaoContext daoContext)
	{
		this.entityMetaMap = entityMetaMap;
		this.configContext = configContext;
		this.daoContext = daoContext;
	}

	public <T> SliceQuery<T> sliceQueryFor(Class<T> entityClass)
	{
		Validator.validateNotNull(entityClass, "Entity class should be provided for Slice Query");
		EntityMeta entityMeta = entityMetaMap.get(entityClass);

		Validator.validateNotNull(entityMeta,
				"Cannot found meta data for entity '" + entityClass.getCanonicalName()
						+ "'. Are you sure this entity is managed ?");

		return new SliceQuery<T>(entityMeta, configContext, daoContext, entityClass);
	}
}
