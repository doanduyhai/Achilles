package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JoinExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapper<K, V> extends AbstractWideMapWrapper<K, V>
{

	private static final Logger log = LoggerFactory.getLogger(JoinWideMapWrapper.class);

	private Object id;
	private PropertyMeta<K, V> propertyMeta;
	private ThriftGenericWideRowDao dao;
	private ThriftEntityPersister persister;
	private ThriftEntityLoader loader;
	private AchillesEntityProxifier proxifier;
	private CompositeHelper compositeHelper;
	private CompositeFactory compositeFactory;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;

	private Composite buildComposite(K key)
	{
		return compositeFactory.createBaseComposite(propertyMeta, key);
	}

	@Override
	public V get(K key)
	{
		log.trace("Get join value having key {}", key);
		V result = null;
		Object joinId = dao.getValue(id, buildComposite(key));
		if (joinId != null)
		{
			EntityMeta joinMeta = propertyMeta.joinMeta();

			AchillesPersistenceContext joinContext = context.newPersistenceContext(
					propertyMeta.getValueClass(), joinMeta, joinId);

			result = loader.<V> load(joinContext, propertyMeta.getValueClass());
			result = proxifier.buildProxy(result, joinContext);
		}
		return result;
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		log.trace("Insert join value {} with key {} and ttl {}", value, key, ttl);

		Object joinId = persistOrEnsureJoinEntityExists(value);
		dao.setValueBatch(id, buildComposite(key), joinId, ttl,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void insert(K key, V value)
	{
		log.trace("Insert join value {} with key {}", value, key);

		Object joinId = persistOrEnsureJoinEntityExists(value);
		dao.setValueBatch(id, buildComposite(key), joinId,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace(
					"Find key/join value pairs in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}

		List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createJoinKeyValueList(context, propertyMeta, hColumns);
	}

	@Override
	public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);
		if (log.isTraceEnabled())
		{
			log.trace("Find join values in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}
		List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createJoinValueList(context, propertyMeta, hColumns);
	}

	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find keys in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}

		List<HColumn<Composite, Object>> hColumns = dao.findRawColumnsRange(id, queryComps[0],
				queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createKeyList(propertyMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				ordering);

		if (log.isTraceEnabled())
		{
			log.trace(
					"Iterate in range {} / {} with bounding {} and ordering {} and batch of {} elements",
					format(composites[0]), format(composites[1]), bounds.name(), ordering.name(),
					count);
		}

		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(propertyMeta.joinMeta()
				.getColumnFamilyName());

		AchillesJoinSliceIterator<?, K, V> joinColumnSliceIterator = dao.getJoinColumnsIterator(
				joinEntityDao, propertyMeta, id, composites[0], composites[1],
				ordering.isReverse(), count);

		return iteratorFactory.createJoinKeyValueIterator(context, joinColumnSliceIterator,
				propertyMeta);
	}

	@Override
	public void remove(K key)
	{
		log.trace("Remove join value having key {}", key);

		dao.removeColumnBatch(id, buildComposite(key),
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);

		Composite[] queryComps = compositeFactory.createForQuery(//
				propertyMeta, start, end, bounds, OrderingMode.ASCENDING);
		if (log.isTraceEnabled())
		{
			log.trace("Remove join values in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(),
					OrderingMode.ASCENDING.name());
		}
		dao.removeColumnRangeBatch(id, queryComps[0], queryComps[1],
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void removeFirst(int count)
	{
		log.trace("Remove first {} join values", count);
		dao.removeColumnRangeBatch(id, null, null, false, count,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void removeLast(int count)
	{
		log.trace("Remove last {} join values", count);
		dao.removeColumnRangeBatch(id, null, null, true, count,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	private Object persistOrEnsureJoinEntityExists(V value)
	{
		Object joinId = null;
		JoinProperties joinProperties = propertyMeta.getJoinProperties();

		if (value != null)
		{
			ThriftPersistenceContext joinContext = (ThriftPersistenceContext) context
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
		return propertyMeta.getExternalCFName();
	}

	public void setId(Object id)
	{
		this.id = id;
	}

	public void setExternalWideMapMeta(PropertyMeta<K, V> externalWideMapMeta)
	{
		this.propertyMeta = externalWideMapMeta;
	}

	public void setEntityProxifier(AchillesEntityProxifier proxifier)
	{
		this.proxifier = proxifier;
	}

	public void setPersister(ThriftEntityPersister persister)
	{
		this.persister = persister;
	}

	public void setLoader(ThriftEntityLoader loader)
	{
		this.loader = loader;
	}

	public void setCompositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
	}

	public void setCompositeKeyFactory(CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
	}

	public void setKeyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
	}

	public void setIteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
	}

	public void setDao(ThriftGenericWideRowDao dao)
	{
		this.dao = dao;
	}
}
