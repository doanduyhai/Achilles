package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExternalWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<K, V> extends AbstractWideMapWrapper<K, V>
{

	private static final Logger log = LoggerFactory.getLogger(WideMapWrapper.class);

	protected Object id;
	protected ThriftGenericWideRowDao dao;
	protected PropertyMeta<K, V> propertyMeta;
	private CompositeHelper compositeHelper;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;
	private CompositeFactory compositeFactory;

	protected Composite buildComposite(K key)
	{
		Composite comp = compositeFactory.createBaseComposite(propertyMeta, key);
		return comp;
	}

	@Override
	public V get(K key)
	{
		log.trace("Get value having key {}", key);

		V result = null;
		Object value = dao.getValue(id, buildComposite(key));
		if (value != null)
		{
			result = propertyMeta.castValue(value);
		}
		return result;
	}

	@Override
	public void insert(K key, V value)
	{
		log.trace("Insert value {} with key {}", value, key);

		dao.setValueBatch(id, buildComposite(key),
				propertyMeta.writeValueAsSupportedTypeOrString(value),
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		log.trace("Insert value {} with key {} and ttl {}", value, key, ttl);

		dao.setValueBatch(id, buildComposite(key),
				propertyMeta.writeValueAsSupportedTypeOrString(value), ttl,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find key/value pairs in range {} / {} with bounding {} and ordering {}",
					format(composites[0]), format(composites[1]), bounds.name(), ordering.name());
		}

		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], count, ordering.isReverse());

		return keyValueFactory.createKeyValueList(context, propertyMeta, hColumns);
	}

	@Override
	public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find value in range {} / {} with bounding {} and ordering {}",
					format(composites[0]), format(composites[1]), bounds.name(), ordering.name());
		}
		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], count, ordering.isReverse());

		return keyValueFactory.createValueList(propertyMeta, hColumns);
	}

	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find keys in range {} / {} with bounding {} and ordering {}",
					format(composites[0]), format(composites[1]), bounds.name(), ordering.name());
		}
		List<HColumn<Composite, V>> hColumns = dao.findRawColumnsRange(id, composites[0],
				composites[1], count, ordering.isReverse());

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

		AchillesSliceIterator<?, V> columnSliceIterator = dao.getColumnsIterator(id, composites[0],
				composites[1], ordering.isReverse(), count);

		return iteratorFactory.createKeyValueIterator(context, columnSliceIterator, propertyMeta);

	}

	@Override
	public void remove(K key)
	{
		log.trace("Remove value having key {}", key);

		dao.removeColumnBatch(id, buildComposite(key),
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);
		Composite[] composites = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				OrderingMode.ASCENDING);

		if (log.isTraceEnabled())
		{
			log.trace("Remove values in range {} / {} with bounding {} and ordering {}",
					format(composites[0]), format(composites[1]), bounds.name(),
					OrderingMode.ASCENDING.name());
		}

		dao.removeColumnRangeBatch(id, composites[0], composites[1],
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void removeFirst(int count)
	{
		log.trace("Remove first {} values", count);

		dao.removeColumnRangeBatch(id, null, null, false, count,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	@Override
	public void removeLast(int count)
	{
		log.trace("Remove last {} values", count);

		dao.removeColumnRangeBatch(id, null, null, true, count,
				context.getWideRowMutator(getExternalCFName()));
		context.flush();
	}

	private String getExternalCFName()
	{
		return propertyMeta.getExternalCFName();
	}

	public void setId(Object id)
	{
		this.id = id;
	}

	public void setDao(ThriftGenericWideRowDao dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.propertyMeta = wideMapMeta;
	}

	public void setCompositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
	}

	public void setKeyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
	}

	public void setIteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
	}

	public void setCompositeKeyFactory(CompositeFactory compositeFactory)
	{
		this.compositeFactory = compositeFactory;
	}
}
