package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP_COUNTER;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.Factory;

/**
 * EntityBatcher
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityBatcher
{
	private EntityProxifier proxifier = new EntityProxifier();

	@SuppressWarnings("unchecked")
	public <T, ID> void startBatchForEntity(T entity, PersistenceContext<ID> context)
	{
		EntityMeta<ID> entityMeta = context.getEntityMeta();
		Mutator<ID> mutator;
		Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>>();

		for (PropertyMeta<?, ?> propertyMeta : entityMeta.getPropertyMetas().values())
		{
			String propertyName = propertyMeta.getPropertyName();
			if (propertyMeta.isJoin())
			{
				String joinColumnFamilyName = propertyMeta.joinMeta().getColumnFamilyName();
				AbstractDao<?, ?, ?> dao = context.findEntityDao(joinColumnFamilyName);
				Mutator<?> joinMutator = dao.buildMutator();
				mutatorMap.put(propertyName, new Pair<Mutator<?>, AbstractDao<?, ?, ?>>(
						joinMutator, dao));
			}
			else if (propertyMeta.type() == WIDE_MAP_COUNTER)
			{

				CounterDao counterDao = context.getCounterDao();
				Mutator<Composite> counterMutator = counterDao.buildMutator();

				mutatorMap.put(propertyName, new Pair<Mutator<?>, AbstractDao<?, ?, ?>>(
						counterMutator, counterDao));
			}
		}

		mutator = context.getEntityDao().buildMutator();

		Factory proxy = (Factory) entity;
		JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) proxy
				.getCallback(0);

		interceptor.setMutator(mutator);
		interceptor.setMutatorMap(mutatorMap);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	public <T, ID, JOIN_ID> void endBatch(T entity, PersistenceContext<ID> context)
	{
		JpaEntityInterceptor<ID, T> interceptor = proxifier.getInterceptor(entity);

		Mutator<ID> mutator = interceptor.getMutator();

		if (mutator != null)
		{
			context.getEntityDao().executeMutator(mutator);
		}
		for (Entry<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> entry : interceptor
				.getMutatorMap().entrySet())
		{
			Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = entry.getValue();
			if (pair != null)
			{
				pair.right.executeMutator((Mutator) pair.left);
			}
		}
		interceptor.setMutatorMap(null);
	}
}
