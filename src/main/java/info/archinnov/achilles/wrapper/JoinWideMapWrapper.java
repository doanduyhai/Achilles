package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * JoinWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapper<ID, JOIN_ID, K, V> extends WideMapWrapper<ID, K, V>
{

	private EntityPersister persister;
	private EntityLoader loader;

	@SuppressWarnings("unchecked")
	@Override
	public V get(K key)
	{
		String joinId = context.getEntityDao().getValue(id, buildComposite(key));
		if (joinId != null)
		{
			PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
					.joinIdMeta();

			PersistenceContext<?> joinContext = context.newPersistenceContext(
					propertyMeta.getValueClass(), (EntityMeta<JOIN_ID>) propertyMeta.joinMeta(),
					joinIdMeta.getValueFromString(joinId));

			V entity = (V) loader.load(joinContext);

			return proxifier.buildProxy(entity, joinContext);
		}
		else
		{
			return null;
		}
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();
		context.getEntityDao().setValueBatch(id, buildComposite(key),
				joinIdMeta.writeValueToString(joinId), ttl, interceptor.getMutator());
		context.flush();
	}

	@Override
	public void insert(K key, V value)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();
		context.getEntityDao().setValueBatch(id, buildComposite(key),
				joinIdMeta.writeValueToString(joinId), interceptor.getMutator());
		context.flush();
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);
		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = context.findEntityDao(propertyMeta
				.joinMeta().getColumnFamilyName());

		AchillesJoinSliceIterator<ID, DynamicComposite, String, JOIN_ID, K, V> joinColumnSliceIterator = context
				.getEntityDao().getJoinColumnsIterator(joinEntityDao, propertyMeta, id,
						queryComps[0], queryComps[1], ordering.isReverse(), count);

		return iteratorFactory.createKeyValueJoinIteratorForDynamicComposite(context,
				joinColumnSliceIterator, propertyMeta);
	}

	@SuppressWarnings("unchecked")
	private Object persistOrEnsureJoinEntityExists(V value)
	{
		Object joinId = null;
		JoinProperties joinProperties = propertyMeta.getJoinProperties();

		if (value != null)
		{
			PersistenceContext<JOIN_ID> joinContext = (PersistenceContext<JOIN_ID>) context
					.newPersistenceContext(propertyMeta.joinMeta(), value);

			joinId = persister.cascadePersistOrEnsureExists(joinContext, value, joinProperties);
		}
		else
		{
			throw new IllegalArgumentException("Cannot persist null entity");
		}
		return joinId;
	}

	public void setPersister(EntityPersister persister)
	{
		this.persister = persister;
	}

	public void setLoader(EntityLoader loader)
	{
		this.loader = loader;
	}
}
