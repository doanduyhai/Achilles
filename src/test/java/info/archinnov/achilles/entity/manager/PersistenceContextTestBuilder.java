package info.archinnov.achilles.entity.manager;

import static org.mockito.Mockito.mock;
import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.mutation.Mutator;

import org.powermock.reflect.Whitebox;

/**
 * PersistenceContextTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class PersistenceContextTestBuilder<ID>
{
	private EntityMeta<ID> entityMeta;
	private Map<String, GenericDynamicCompositeDao<?>> entityDaosMap = new HashMap<String, GenericDynamicCompositeDao<?>>();
	private Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericCompositeDao<?, ?>>();
	private CounterDao counterDao;
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;
	private GenericDynamicCompositeDao<ID> entityDao;
	private GenericCompositeDao<ID, ?> columnFamilyDao;

	private Mutator<ID> mutator;
	private boolean pendingBatch = false;
	private GenericDynamicCompositeDao<?> joinEntityDao;
	private Mutator<?> joinMutator;

	public static <ID, T> PersistenceContextTestBuilder<ID> context(EntityMeta<ID> entityMeta,//
			CounterDao counterDao, //
			AchillesConfigurableConsistencyLevelPolicy policy, //
			Class<T> entityClass, ID primaryKey)
	{
		return new PersistenceContextTestBuilder<ID>(entityMeta, counterDao, policy, entityClass,
				primaryKey);
	}

	public static <ID, T> PersistenceContextTestBuilder<ID> mockAll(EntityMeta<ID> entityMeta,
			Class<T> entityClass, ID primaryKey)
	{
		return new PersistenceContextTestBuilder<ID>(entityMeta, mock(CounterDao.class),
				mock(AchillesConfigurableConsistencyLevelPolicy.class), entityClass, primaryKey);
	}

	public PersistenceContextTestBuilder(EntityMeta<ID> entityMeta, CounterDao counterDao,
			AchillesConfigurableConsistencyLevelPolicy policy, Class<?> entityClass, ID primaryKey)
	{
		this.entityMeta = entityMeta;
		this.counterDao = counterDao;
		this.policy = policy;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
	}

	public PersistenceContext<ID> build()
	{
		PersistenceContext<ID> context = new PersistenceContext<ID>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entityClass, primaryKey);

		context.setEntity(entity);
		Whitebox.setInternalState(context, "entityDao", entityDao);
		Whitebox.setInternalState(context, "columnFamilyDao", columnFamilyDao);
		context.setMutator(mutator);
		Whitebox.setInternalState(context, "pendingBatch", pendingBatch);
		Whitebox.setInternalState(context, "joinEntityDao", joinEntityDao);
		Whitebox.setInternalState(context, "joinMutator", joinMutator);
		return context;
	}

	public PersistenceContextTestBuilder<ID> entityDaosMap(
			Map<String, GenericDynamicCompositeDao<?>> entityDaosMap)
	{
		this.entityDaosMap = entityDaosMap;
		return this;
	}

	public PersistenceContextTestBuilder<ID> columnFamilyDaosMap(
			Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap)
	{
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		return this;
	}

	public PersistenceContextTestBuilder<ID> entity(Object entity)
	{
		this.entity = entity;
		return this;
	}

	public PersistenceContextTestBuilder<ID> entityDao(GenericDynamicCompositeDao<ID> entityDao)
	{
		this.entityDao = entityDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> columnFamilyDao(
			GenericCompositeDao<ID, ?> columnFamilyDao)
	{
		this.columnFamilyDao = columnFamilyDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> mutator(Mutator<ID> mutator)
	{
		this.mutator = mutator;
		return this;
	}

	public PersistenceContextTestBuilder<ID> pendingBatch(boolean pendingBatch)
	{
		this.pendingBatch = pendingBatch;
		return this;
	}

	public PersistenceContextTestBuilder<ID> joinEntityDao(
			GenericDynamicCompositeDao<?> joinEntityDao)
	{
		this.joinEntityDao = joinEntityDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> joinMutator(Mutator<?> joinMutator)
	{
		this.joinMutator = joinMutator;
		return this;
	}

}
