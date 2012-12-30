package fr.doan.achilles.wrapper;

import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.entity.operations.EntityPersister;

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

	@Override
	public V get(K key)
	{
		Object joinId = dao.getValue(id, buildComposite(key));
		return (V) loader.loadJoinEntity(wideMapMeta.getValueClass(), joinId, wideMapMeta
				.getJoinProperties().getEntityMeta());
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		dao.setValue(id, buildComposite(key), (Object) joinId, ttl);
	}

	@Override
	public void insert(K key, V value)
	{
		Object joinId = persistOrEnsureJoinEntityExists(value);
		dao.setValue(id, buildComposite(key), (Object) joinId);
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
