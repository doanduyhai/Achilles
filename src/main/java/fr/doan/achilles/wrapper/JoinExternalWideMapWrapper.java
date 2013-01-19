package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.composite.factory.CompositeKeyFactory;
import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.entity.operations.EntityPersister;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.entity.type.WideRow;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * JoinExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> implements WideRow<K, V>
{
	private ID id;
	private GenericCompositeDao<ID, JOIN_ID> externalWideMapDao;
	private PropertyMeta<K, V> externalWideMapMeta;

	private EntityPersister persister = new EntityPersister();
	private EntityLoader loader = new EntityLoader();
	private CompositeHelper helper = new CompositeHelper();

	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();

	private Composite buildComposite(K key)
	{
		Composite comp = compositeKeyFactory.createBaseComposite(externalWideMapMeta, key);
		return comp;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(K key)
	{
		JOIN_ID joinId = externalWideMapDao.getValue(id, buildComposite(key));
		return (V) loader.loadJoinEntity(externalWideMapMeta.getValueClass(), joinId,
				externalWideMapMeta.getJoinProperties().getEntityMeta());
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		JOIN_ID joinId = persistOrEnsureJoinEntityExists(value);
		externalWideMapDao.setValue(id, buildComposite(key), joinId, ttl);
	}

	@Override
	public void insert(K key, V value)
	{
		JOIN_ID joinId = persistOrEnsureJoinEntityExists(value);
		externalWideMapDao.setValue(id, buildComposite(key), joinId);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, K end, boolean reverse, int count)
	{
		return findRange(start, end, true, reverse, count);
	}

	@Override
	public List<KeyValue<K, V>> findRange(K start, K end, boolean inclusiveBounds, boolean reverse,
			int count)
	{
		return findRange(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{
		helper.checkBounds(externalWideMapMeta, start, end, reverse);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				externalWideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, JOIN_ID>> hColumns = externalWideMapDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return keyValueFactory.createListForWideRowOrExternalWideMapMeta(externalWideMapMeta,
				(List) hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, boolean reverse, int count)
	{
		return iterator(start, end, true, reverse, count);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
			boolean reverse, int count)
	{
		return iterator(start, inclusiveBounds, end, inclusiveBounds, reverse, count);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{
		Composite[] composites = compositeKeyFactory.createForQuery(externalWideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		ColumnSliceIterator<ID, Composite, JOIN_ID> columnSliceIterator = externalWideMapDao
				.getColumnsIterator(id, composites[0], composites[1], reverse, count);

		return iteratorFactory.createKeyValueIteratorForWideRow(columnSliceIterator,
				externalWideMapMeta);
	}

	@Override
	public void remove(K key)
	{
		externalWideMapDao.removeColumn(id, buildComposite(key));

	}

	@Override
	public void removeRange(K start, K end)
	{
		removeRange(start, end, true);
	}

	@Override
	public void removeRange(K start, K end, boolean inclusiveBounds)
	{
		removeRange(start, inclusiveBounds, end, inclusiveBounds);
	}

	@Override
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{
		helper.checkBounds(externalWideMapMeta, start, end, false);

		Composite[] queryComps = compositeKeyFactory.createForQuery(//
				externalWideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		externalWideMapDao.removeColumnRange(id, queryComps[0], queryComps[1]);

	}

	private JOIN_ID persistOrEnsureJoinEntityExists(V value)
	{
		JOIN_ID joinId = null;
		JoinProperties joinProperties = externalWideMapMeta.getJoinProperties();

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

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setExternalWideMapDao(GenericCompositeDao<ID, JOIN_ID> externalWideMapDao)
	{
		this.externalWideMapDao = externalWideMapDao;
	}

	public void setExternalWideMapMeta(PropertyMeta<K, V> externalWideMapMeta)
	{
		this.externalWideMapMeta = externalWideMapMeta;
	}
}
