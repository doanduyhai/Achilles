package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import me.prettyprint.hector.api.mutation.Mutator;

/**
 * PersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class PersistenceContext<ID>
{
	private final EntityIntrospector introspector = new EntityIntrospector();
	private final EntityMeta<ID> entityMeta;
	private final Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;
	private final Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap;
	private final CounterDao counterDao;
	private final AchillesConfigurableConsistencyLevelPolicy policy;

	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;
	private GenericDynamicCompositeDao<ID> entityDao;
	private GenericCompositeDao<ID, ?> columnFamilyDao;

	// Batch
	private Mutator<ID> mutator;
	private boolean pendingBatch = false;
	private GenericDynamicCompositeDao<?> joinEntityDao;
	private Mutator<?> joinMutator;

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

		this.primaryKey = introspector.getKey(entity, entityMeta.getIdMeta());

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");

		this.initDaos();
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

		Validator.validateNotNull(primaryKey, "The primary key for the entity '" + entity
				+ "' should not be null");

		this.initDaos();
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(
			EntityMeta<JOIN_ID> joinMeta, Object joinEntity)
	{
		PersistenceContext<JOIN_ID> context = new PersistenceContext<JOIN_ID>(joinMeta,
				entityDaosMap, columnFamilyDaosMap, counterDao, policy, joinEntity);
		context.setPrimaryKey(introspector.getKey(joinEntity, joinMeta.getIdMeta()));
		return context;
	}

	public <JOIN_ID> PersistenceContext<JOIN_ID> newPersistenceContext(Class<?> entityClass,
			EntityMeta<JOIN_ID> joinMeta, JOIN_ID joinId)
	{
		return new PersistenceContext<JOIN_ID>(joinMeta, entityDaosMap, columnFamilyDaosMap,
				counterDao, policy, entityClass, joinId);
	}

	public void startBatch()
	{
		if (!pendingBatch)
		{
			this.mutator = this.getEntityDao().buildMutator();
		}
	}

	public void endBatch()
	{
		if (!pendingBatch)
		{
			this.getEntityDao().executeMutator(mutator);
			this.mutator = null;
		}
	}

	public void startBatchForJoin(String joinColumnFamilyName)
	{
		this.joinEntityDao = this.findEntityDao(joinColumnFamilyName);
		this.joinMutator = joinEntityDao.buildMutator();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public void endBatchForJoin()
	{
		this.joinEntityDao.executeMutator((Mutator) joinMutator);
		this.joinMutator = null;
		this.joinEntityDao = null;
	}

	public void joinBatch(Mutator<ID> extistingMutator)
	{
		this.mutator = extistingMutator;
		this.pendingBatch = true;
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

	public GenericDynamicCompositeDao<ID> getEntityDao()
	{
		return entityDao;
	}

	public GenericCompositeDao<ID, ?> getColumnFamilyDao()
	{
		return columnFamilyDao;
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

	public Mutator<ID> getMutator()
	{
		return mutator;
	}

	public void setMutator(Mutator<ID> mutator)
	{
		this.mutator = mutator;
	}

	@SuppressWarnings("unchecked")
	private void initDaos()
	{
		String columnFamilyName = entityMeta.getColumnFamilyName();
		if (entityMeta.isColumnFamilyDirectMapping())
		{
			this.columnFamilyDao = (GenericCompositeDao<ID, ?>) columnFamilyDaosMap
					.get(columnFamilyName);
		}
		else
		{
			this.entityDao = (GenericDynamicCompositeDao<ID>) entityDaosMap.get(columnFamilyName);
		}
	}

	public Mutator<?> getJoinMutator()
	{
		return joinMutator;
	}
}
