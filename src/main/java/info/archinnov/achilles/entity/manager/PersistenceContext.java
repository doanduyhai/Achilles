package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

/**
 * PersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class PersistenceContext<ID>
{
	private final EntityHelper helper = new EntityHelper();

	private final EntityMeta<ID> entityMeta;
	private final Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;
	private final Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap;
	private final CounterDao counterDao;
	private final AchillesConfigurableConsistencyLevelPolicy policy;

	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;

	public PersistenceContext(EntityMeta<ID> entityMeta,
			Map<String, GenericDynamicCompositeDao<?>> entityDaosMap,
			Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap, //
			CounterDao counterDao, //
			AchillesConfigurableConsistencyLevelPolicy policy, //
			Object entity)
	{
		this.entityMeta = entityMeta;
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
		this.policy = policy;
		this.entity = entity;
		this.entityClass = entity.getClass();
		this.primaryKey = helper.getKey(entity, entityMeta.getIdMeta());

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");
	}

	public PersistenceContext(EntityMeta<ID> entityMeta,
			Map<String, GenericDynamicCompositeDao<?>> entityDaosMap,
			Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap, CounterDao counterDao, //
			AchillesConfigurableConsistencyLevelPolicy policy, //
			Class<?> entityClass, ID primaryKey)
	{
		this.entityMeta = entityMeta;
		this.entityDaosMap = entityDaosMap;
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		this.counterDao = counterDao;
		this.policy = policy;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(
			EntityMeta<JOIN_ID> joinMeta, Object joinEntity)
	{
		PersistenceContext<JOIN_ID> context = new PersistenceContext<JOIN_ID>(joinMeta,
				entityDaosMap, columnFamilyDaosMap, counterDao, policy, joinEntity);
		context.setPrimaryKey(helper.getKey(joinEntity, joinMeta.getIdMeta()));
		return context;
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(Class<?> entityClass,
			EntityMeta<JOIN_ID> joinMeta, JOIN_ID joinId)
	{
		return new PersistenceContext<JOIN_ID>(joinMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, policy, entityClass, joinId);
	}

	public EntityMeta<ID> getEntityMeta()
	{
		return entityMeta;
	}

	public Map<String, GenericDynamicCompositeDao<?>> getEntityDaosMap()
	{
		return entityDaosMap;
	}

	public Map<String, GenericCompositeDao<?, ?>> getColumnFamilyDaosMap()
	{
		return columnFamilyDaosMap;
	}

	public Object getEntity()
	{
		return entity;
	}

	public void setEntity(Object entity)
	{
		this.entity = entity;
	}

	public CounterDao getCounterDao()
	{
		return counterDao;
	}

	@SuppressWarnings("unchecked")
	public GenericDynamicCompositeDao<ID> fetchEntityDao()
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		return (GenericDynamicCompositeDao<ID>) entityDaosMap.get(columnFamilyName);
	}

	@SuppressWarnings("unchecked")
	public GenericCompositeDao<ID, ?> fetchColumnFamilyDao()
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		return (GenericCompositeDao<ID, ?>) columnFamilyDaosMap.get(columnFamilyName);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID> GenericDynamicCompositeDao<JOIN_ID> findEntityDao(String columnFamilyName)
	{
		return (GenericDynamicCompositeDao<JOIN_ID>) entityDaosMap.get(columnFamilyName);
	}

	@SuppressWarnings("unchecked")
	public <JOIN_ID, V> GenericCompositeDao<JOIN_ID, V> findColumnFamilyDao(String columnFamilyName)
	{
		return (GenericCompositeDao<JOIN_ID, V>) columnFamilyDaosMap.get(columnFamilyName);
	}

	public Class<?> getEntityClass()
	{
		return entityClass;
	}

	public ID getPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(ID primaryKey)
	{
		this.primaryKey = primaryKey;
	}

	public boolean isDirectColumnFamilyMapping()
	{
		return this.entityMeta.isColumnFamilyDirectMapping();
	}

	public AchillesConfigurableConsistencyLevelPolicy getPolicy()
	{
		return policy;
	}
}
