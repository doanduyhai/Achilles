package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * JoinExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinExternalWideMapWrapper<ID, JOIN_ID, K, V> extends AbstractWideMapWrapper<K, V>
{
	private ID id;
	private GenericCompositeDao<ID, JOIN_ID> dao;
	private PropertyMeta<K, V> propertyMeta;

	private EntityPersister persister;
	private EntityLoader loader;
	private EntityHelper entityHelper;
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
		PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
				.joinIdMeta();
		V entity = (V) loader.load(propertyMeta.getValueClass(), joinIdMeta.castValue(joinId),
				joinMeta);

		return entityHelper.buildProxy(entity, propertyMeta.joinMeta());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value, int ttl)
	{
		JOIN_ID joinId = (JOIN_ID) persistOrEnsureJoinEntityExists(value);

		if (this.interceptor.isBatchMode())
		{
			dao.setValueBatch(id, buildComposite(key), joinId, ttl,
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			dao.setValue(id, buildComposite(key), joinId, ttl);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void insert(K key, V value)
	{
		JOIN_ID joinId = (JOIN_ID) persistOrEnsureJoinEntityExists(value);
		if (this.interceptor.isBatchMode())
		{
			dao.setValueBatch(id, buildComposite(key), joinId,
					(Mutator<ID>) interceptor.getMutator());
		}
		else
		{
			dao.setValue(id, buildComposite(key), joinId);
		}
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<Composite, JOIN_ID>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.asBoolean());

		return keyValueFactory.createJoinKeyValueListForComposite(propertyMeta, (List) hColumns);
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
				queryComps[1], count, ordering.asBoolean());

		return keyValueFactory.createJoinValueListForComposite(propertyMeta, (List) hColumns);
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
				queryComps[1], count, ordering.asBoolean());

		return keyValueFactory.createKeyListForComposite(propertyMeta, (List) hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		Composite[] composites = compositeKeyFactory.createForQuery(propertyMeta, start, end, 
				bounds, ordering);

		AchillesJoinSliceIterator<ID, Composite, JOIN_ID, K, V> joinColumnSliceIterator = dao
				.getJoinColumnsIterator(propertyMeta, id, composites[0], composites[1], ordering.asBoolean(),
						count);

		return iteratorFactory.createKeyValueJoinIteratorForComposite(joinColumnSliceIterator,
				propertyMeta);
	}

	@Override
	public void remove(K key)
	{
		dao.removeColumn(id, buildComposite(key));

	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);

		Composite[] queryComps = compositeKeyFactory.createForQuery(//
				propertyMeta, start, end, bounds, OrderingMode.ASCENDING);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);

	}

	@Override
	public void removeFirst(int count)
	{
		dao.removeColumnRange(id, null, null, false, count);

	}

	@Override
	public void removeLast(int count)
	{
		dao.removeColumnRange(id, null, null, true, count);
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

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setExternalWideMapDao(GenericCompositeDao<ID, JOIN_ID> externalWideMapDao)
	{
		this.dao = externalWideMapDao;
	}

	public void setExternalWideMapMeta(PropertyMeta<K, V> externalWideMapMeta)
	{
		this.propertyMeta = externalWideMapMeta;
	}

	public void setEntityHelper(EntityHelper entityHelper)
	{
		this.entityHelper = entityHelper;
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

}
