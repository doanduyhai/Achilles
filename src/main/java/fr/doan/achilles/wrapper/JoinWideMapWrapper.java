package fr.doan.achilles.wrapper;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.REMOVE;

import javax.persistence.CascadeType;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.EntityMeta;
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
		"unchecked",
		"rawtypes"
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

	@Override
	public void remove(K key)
	{
		JoinProperties<?> joinProperties = wideMapMeta.getJoinProperties();
		DynamicComposite composite = buildComposite(key);

		CascadeType cascadeType = joinProperties.getCascadeType();
		if (cascadeType != null && (cascadeType == REMOVE || cascadeType == ALL))
		{
			Object joinId = dao.getValue(id, composite);
			persister.removeById(joinId, (EntityMeta) joinProperties.getEntityMeta());
		}
		dao.removeColumn(id, composite);
	}

	@Override
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{

		helper.checkBounds(wideMapMeta, start, end, false);

		DynamicComposite[] queryComps = keyFactory.createForQuery(//
				wideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		JoinProperties<?> joinProperties = wideMapMeta.getJoinProperties();
		CascadeType cascadeType = joinProperties.getCascadeType();
		if (cascadeType != null && (cascadeType == REMOVE || cascadeType == ALL))
		{
			ColumnSliceIterator<ID, DynamicComposite, Object> iterator = dao.getColumnsIterator(id,
					queryComps[0], queryComps[1], false);
			while (iterator.hasNext())
			{
				persister.removeById(iterator.next().getValue(),
						(EntityMeta) joinProperties.getEntityMeta());
			}
		}
		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}

	private Object persistOrEnsureJoinEntityExists(V value)
	{
		Object joinId = null;
		JoinProperties<?> joinProperties = wideMapMeta.getJoinProperties();

		if (value != null)
		{
			joinId = persister.persistOrEnsureJoinEntityExists(value, joinProperties);
		}
		else
		{
			throw new IllegalArgumentException("Cannot persist null entity");
		}
		return joinId;
	}

}
