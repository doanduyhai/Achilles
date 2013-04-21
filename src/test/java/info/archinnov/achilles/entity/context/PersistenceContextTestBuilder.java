package info.archinnov.achilles.entity.context;

import static org.mockito.Mockito.mock;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.HashMap;
import java.util.Map;

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
	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericColumnFamilyDao<?, ?>>();
	private CounterDao counterDao;
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private Object entity;
	private Class<?> entityClass;
	private ID primaryKey;
	private GenericEntityDao<ID> entityDao;
	private GenericColumnFamilyDao<ID, ?> columnFamilyDao;

	private ImmediateFlushContext immediateFlushContext;

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
		PersistenceContext<ID> context = new PersistenceContext<ID>(//
				entityMeta, //
				entityDaosMap, //
				columnFamilyDaosMap, //
				counterDao, //
				policy, //
				immediateFlushContext, //
				entityClass, primaryKey);

		context.setEntity(entity);
		Whitebox.setInternalState(context, "entityDao", entityDao);
		Whitebox.setInternalState(context, "columnFamilyDao", columnFamilyDao);
		return context;
	}

	public PersistenceContextTestBuilder<ID> entityDaosMap(
			Map<String, GenericEntityDao<?>> entityDaosMap)
	{
		this.entityDaosMap = entityDaosMap;
		return this;
	}

	public PersistenceContextTestBuilder<ID> columnFamilyDaosMap(
			Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap)
	{
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		return this;
	}

	public PersistenceContextTestBuilder<ID> entity(Object entity)
	{
		this.entity = entity;
		return this;
	}

	public PersistenceContextTestBuilder<ID> entityDao(GenericEntityDao<ID> entityDao)
	{
		this.entityDao = entityDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> columnFamilyDao(
			GenericColumnFamilyDao<ID, ?> columnFamilyDao)
	{
		this.columnFamilyDao = columnFamilyDao;
		return this;
	}

	public PersistenceContextTestBuilder<ID> immediateFlushContext(ImmediateFlushContext immediateFlushContext)
	{
		this.immediateFlushContext = immediateFlushContext;
		return this;
	}

}
