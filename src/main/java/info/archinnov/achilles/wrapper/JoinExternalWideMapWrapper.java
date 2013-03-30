package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * JoinExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> extends AbstractWideMapWrapper<ID, K, V>
{
	private ID id;
	private PropertyMeta<K, V> propertyMeta;
	private GenericCompositeDao<ID, JOIN_ID> dao;
	private EntityPersister persister;
	private EntityLoader loader;
	private EntityProxifier proxifier;
	private CompositeHelper compositeHelper;
	private CompositeKeyFactory compositeKeyFactory;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;

	private Composite buildComposite(K key)
	{
		Composite comp = compositeKeyFactory.createBaseComposite(propertyMeta, key);
		return comp;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(K key)
	{
		JOIN_ID joinId = dao.getValue(id, buildComposite(key));
		EntityMeta<JOIN_ID> joinMeta = (EntityMeta<JOIN_ID>) propertyMeta.joinMeta();

		PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(
				propertyMeta.getValueClass(), joinMeta, joinId);

		V entity = loader.load(joinContext);

		return proxifier.buildProxy(entity, joinContext);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value, int ttl)
	{
		JOIN_ID joinId = (JOIN_ID) persistOrEnsureJoinEntityExists(value);
		dao.setValueBatch(id, buildComposite(key), joinId, ttl,
				interceptor.getColumnFamilyMutator(getExternalCFName()));
		context.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value)
	{
		JOIN_ID joinId = (JOIN_ID) persistOrEnsureJoinEntityExists(value);
		dao.setValueBatch(id, buildComposite(key), joinId,
				interceptor.getColumnFamilyMutator(getExternalCFName()));
		context.flush();
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<Composite, JOIN_ID>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createJoinKeyValueListForComposite(context, propertyMeta,
				(List) hColumns);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<Composite, JOIN_ID>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createJoinValueListForComposite(context, propertyMeta,
				(List) hColumns);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<Composite, JOIN_ID>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createKeyListForComposite(propertyMeta, (List) hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		Composite[] composites = compositeKeyFactory.createForQuery(propertyMeta, start, end,
				bounds, ordering);

		GenericDynamicCompositeDao<JOIN_ID> joinEntityDao = context.findEntityDao(propertyMeta
				.joinMeta().getColumnFamilyName());

		AchillesJoinSliceIterator<ID, Composite, ?, JOIN_ID, K, V> joinColumnSliceIterator = dao
				.getJoinColumnsIterator(joinEntityDao, propertyMeta, id, composites[0],
						composites[1], ordering.isReverse(), count);

		return iteratorFactory.createKeyValueJoinIteratorForComposite(context,
				joinColumnSliceIterator, propertyMeta);
	}

	@Override
	public void remove(K key)
	{
		dao.removeColumnBatch(id, buildComposite(key),
				interceptor.getColumnFamilyMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);

		Composite[] queryComps = compositeKeyFactory.createForQuery(//
				propertyMeta, start, end, bounds, OrderingMode.ASCENDING);

		dao.removeColumnRangeBatch(id, queryComps[0], queryComps[1],
				interceptor.getColumnFamilyMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void removeFirst(int count)
	{
		dao.removeColumnRangeBatch(id, null, null, false, count,
				interceptor.getColumnFamilyMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void removeLast(int count)
	{
		dao.removeColumnRangeBatch(id, null, null, true, count,
				interceptor.getColumnFamilyMutator(getExternalCFName()));
		context.flush();
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

	private String getExternalCFName()
	{
		return propertyMeta.getExternalWideMapProperties().getExternalColumnFamilyName();
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setExternalWideMapMeta(PropertyMeta<K, V> externalWideMapMeta)
	{
		this.propertyMeta = externalWideMapMeta;
	}

	public void setEntityProxifier(EntityProxifier proxifier)
	{
		this.proxifier = proxifier;
	}

	public void setPersister(EntityPersister persister)
	{
		this.persister = persister;
	}

	public void setLoader(EntityLoader loader)
	{
		this.loader = loader;
	}

	public void setCompositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
	}

	public void setCompositeKeyFactory(CompositeKeyFactory compositeKeyFactory)
	{
		this.compositeKeyFactory = compositeKeyFactory;
	}

	public void setKeyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
	}

	public void setIteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
	}

	public void setDao(GenericCompositeDao<ID, JOIN_ID> dao)
	{
		this.dao = dao;
	}
}
