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

	private EntityPersister persister = new EntityPersister();
	private EntityLoader loader = new EntityLoader();

	@SuppressWarnings("rawtypes")
	@Override
	public V get(K key)
	{
		String joinId = dao.getValue(id, buildComposite(key));
		EntityMeta entityMeta = wideMapMeta.getJoinProperties().getEntityMeta();

		return (V) loader.loadJoinEntity(wideMapMeta.getValueClass(), entityMeta.getIdMeta()
				.getValueFromString(joinId), entityMeta);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		if (this.interceptor.isBatchMode())
		{
			dao.setValueBatch(id, buildComposite(key), wideMapMeta.writeValueToString(joinId), ttl,
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			dao.setValue(id, buildComposite(key), wideMapMeta.writeValueToString(joinId), ttl);
		}
	}

	@Override
	public void insert(K key, V value)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		if (this.interceptor.isBatchMode())
		{
			dao.setValueBatch(id, buildComposite(key), wideMapMeta.writeValueToString(joinId),
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			dao.setValue(id, buildComposite(key), wideMapMeta.writeValueToString(joinId));
		}
	}

	private Object persistOrEnsureJoinEntityExists(V value)
	{
		Object joinId = null;
		JoinProperties joinProperties = wideMapMeta.getJoinProperties();

		if (value != null)
		{
			joinId = persister.cascadePersistOrEnsureExists(value, joinProperties);
		}
		else
		{
			throw new IllegalArgumentException("Cannot persist null entity");
		}
		return joinId;
	}

}
