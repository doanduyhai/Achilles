package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * JoinWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
	"unchecked"
})
public class JoinWideMapWrapper<ID, K, V> extends WideMapWrapper<ID, K, V>
{

	private EntityPersister persister;
	private EntityLoader loader;

	@SuppressWarnings("rawtypes")
	@Override
	public V get(K key)
	{
		String joinId = entityDao.getValue(id, buildComposite(key));
		EntityMeta entityMeta = propertyMeta.getJoinProperties().getEntityMeta();

		V entity = (V) loader.loadJoinEntity(propertyMeta.getValueClass(), entityMeta.getIdMeta()
				.getValueFromString(joinId), entityMeta);

		return entityHelper.buildProxy(entity, propertyMeta.joinMeta());
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		if (this.interceptor.isBatchMode())
		{
			entityDao.setValueBatch(id, buildComposite(key),
					propertyMeta.writeValueToString(joinId), ttl,
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			entityDao.setValue(id, buildComposite(key), propertyMeta.writeValueToString(joinId),
					ttl);
		}
	}

	@Override
	public void insert(K key, V value)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		if (this.interceptor.isBatchMode())
		{
			entityDao
					.setValueBatch(id, buildComposite(key),
							propertyMeta.writeValueToString(joinId),
							(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			entityDao.setValue(id, buildComposite(key), propertyMeta.writeValueToString(joinId));
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
