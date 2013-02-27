package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * JoinWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapper<ID, K, V> extends WideMapWrapper<ID, K, V>
{

	private EntityPersister persister;
	private EntityLoader loader;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Override
	public V get(K key)
	{
		String joinId = entityDao.getValue(id, buildComposite(key));
		EntityMeta<?> joinMeta = propertyMeta.joinMeta();
		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();
		V entity = (V) loader.load(propertyMeta.getValueClass(),
				joinIdMeta.getValueFromString(joinId), (EntityMeta) joinMeta);

		return entityHelper.buildProxy(entity, propertyMeta.joinMeta());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value, int ttl)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();
		if (this.interceptor.isBatchMode())
		{
			entityDao.setValueBatch(id, buildComposite(key), joinIdMeta.writeValueToString(joinId),
					ttl, (Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			entityDao.setValue(id, buildComposite(key), joinIdMeta.writeValueToString(joinId), ttl);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		PropertyMeta<Void, ?> joinIdMeta = propertyMeta.joinIdMeta();
		if (this.interceptor.isBatchMode())
		{
			entityDao.setValueBatch(id, buildComposite(key), joinIdMeta.writeValueToString(joinId),
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			entityDao.setValue(id, buildComposite(key), joinIdMeta.writeValueToString(joinId));
		}
	}

	private Object persistOrEnsureJoinEntityExists(V value)
	{
		Object joinId = null;
		JoinProperties joinProperties = propertyMeta.getJoinProperties();

		if (value != null)
		{
			if (interceptor.isBatchMode())
			{
				Mutator<?> joinMutator = interceptor.getMutatorForProperty(propertyMeta
						.getPropertyName());
				joinId = persister.cascadePersistOrEnsureExists(value, joinProperties, joinMutator);
			}
			else
			{
				joinId = persister.cascadePersistOrEnsureExists(value, joinProperties);
			}
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
