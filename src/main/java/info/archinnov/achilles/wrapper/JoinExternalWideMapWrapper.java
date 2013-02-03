package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * JoinExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> extends AbstractWideMapWrapper<K, V>
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

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value, int ttl)
	{
		JOIN_ID joinId = (JOIN_ID) persistOrEnsureJoinEntityExists(value);
		externalWideMapDao.setValue(id, buildComposite(key), joinId, ttl);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value)
	{
		JOIN_ID joinId = (JOIN_ID) persistOrEnsureJoinEntityExists(value);
		externalWideMapDao.setValue(id, buildComposite(key), joinId);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		helper.checkBounds(externalWideMapMeta, start, end, reverse);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				externalWideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, JOIN_ID>> hColumns = externalWideMapDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return keyValueFactory.createJoinKeyValueListForComposite(externalWideMapMeta,
				(List) hColumns);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<V> findValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		helper.checkBounds(externalWideMapMeta, start, end, reverse);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				externalWideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, JOIN_ID>> hColumns = externalWideMapDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return keyValueFactory
				.createJoinValueListForComposite(externalWideMapMeta, (List) hColumns);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<K> findKeys(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count)
	{
		helper.checkBounds(externalWideMapMeta, start, end, reverse);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				externalWideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<Composite, JOIN_ID>> hColumns = externalWideMapDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return keyValueFactory.createKeyListForComposite(externalWideMapMeta, (List) hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{
		Composite[] composites = compositeKeyFactory.createForQuery(externalWideMapMeta, start,
				inclusiveStart, end, inclusiveEnd, reverse);

		AchillesJoinSliceIterator<ID, Composite, JOIN_ID, K, V> joinColumnSliceIterator = externalWideMapDao
				.getJoinColumnsIterator(externalWideMapMeta, id, composites[0], composites[1],
						reverse, count);

		return iteratorFactory.createKeyValueIteratorForComposite(joinColumnSliceIterator,
				externalWideMapMeta);
	}

	@Override
	public void remove(K key)
	{
		externalWideMapDao.removeColumn(id, buildComposite(key));

	}

	@Override
	public void remove(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{
		helper.checkBounds(externalWideMapMeta, start, end, false);

		Composite[] queryComps = compositeKeyFactory.createForQuery(//
				externalWideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		externalWideMapDao.removeColumnRange(id, queryComps[0], queryComps[1]);

	}

	@Override
	public void removeFirst(int count)
	{
		externalWideMapDao.removeColumnRange(id, null, null, false, count);

	}

	@Override
	public void removeLast(int count)
	{
		externalWideMapDao.removeColumnRange(id, null, null, true, count);
	}

	private Object persistOrEnsureJoinEntityExists(V value)
	{
		Object joinId = null;
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
