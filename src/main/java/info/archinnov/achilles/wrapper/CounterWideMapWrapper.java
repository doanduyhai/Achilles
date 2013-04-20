package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.helper.LoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesCounterSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CounterWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWideMapWrapper<ID, K> extends AbstractWideMapWrapper<ID, K, Long>
{

	private static final Logger log = LoggerFactory.getLogger(CounterWideMapWrapper.class);

	protected ID id;
	protected String fqcn;
	protected PropertyMeta<Void, ID> idMeta;
	protected GenericColumnFamilyDao<ID, Long> wideMapCounterDao;
	protected PropertyMeta<K, Long> propertyMeta;

	protected CompositeHelper compositeHelper;
	protected KeyValueFactory keyValueFactory;
	protected IteratorFactory iteratorFactory;
	protected CompositeFactory compositeFactory;

	@Override
	public Long get(K key)
	{
		log.trace("Get counter value having key {}", key);
		Composite comp = compositeFactory.createForQuery(propertyMeta, key, EQUAL);
		return wideMapCounterDao.getCounterValue(id, comp);

	}

	@Override
	public void insert(K key, Long value, int ttl)
	{
		throw new UnsupportedOperationException("Cannot insert counter value with ttl");
	}

	@Override
	public void insert(K key, Long value)
	{
		log.trace("Insert counter value {} with key {}", value, key);
		Composite comp = compositeFactory.createBaseComposite(propertyMeta, key);

		wideMapCounterDao.insertCounterBatch(id, comp, value,
				(Mutator<ID>) context.getColumnFamilyMutator(propertyMeta.getExternalCFName()));
		context.flush();
	}

	@Override
	public List<KeyValue<K, Long>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery(propertyMeta, start, end, bounds,
				ordering);
		if (log.isTraceEnabled())
		{
			log.trace("Find key/value pairs in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}

		List<HCounterColumn<Composite>> hColumns = wideMapCounterDao.findCounterColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createCounterKeyValueList(propertyMeta, hColumns);
	}

	@Override
	public List<Long> findValues(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace("Find value in range {} / {} with bounding {} and ordering {}",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name());
		}
		List<HCounterColumn<Composite>> hColumns = wideMapCounterDao.findCounterColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createCounterValueList(propertyMeta, hColumns);
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

		List<HCounterColumn<Composite>> hColumns = wideMapCounterDao.findCounterColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());
		return keyValueFactory.createCounterKeyList(propertyMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, Long> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		Composite[] queryComps = compositeFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		if (log.isTraceEnabled())
		{
			log.trace(
					"Iterate in range {} / {} with bounding {} and ordering {} and batch of {} elements",
					format(queryComps[0]), format(queryComps[1]), bounds.name(), ordering.name(),
					count);
		}
		AchillesCounterSliceIterator<ID> columnSliceIterator = wideMapCounterDao
				.getCounterColumnsIterator(id, queryComps[0], queryComps[1], ordering.isReverse(),
						count);

		return iteratorFactory.createCounterKeyValueIterator(columnSliceIterator, propertyMeta);
	}

	@Override
	public void remove(K key)
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

	@Override
	public void removeFirst(int count)
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

	@Override
	public void removeLast(int count)
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setPropertyMeta(PropertyMeta<K, Long> wideMapMeta)
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

	public void setFqcn(String fqcn)
	{
		this.fqcn = fqcn;
	}

	public void setIdMeta(PropertyMeta<Void, ID> idMeta)
	{
		this.idMeta = idMeta;
	}

	public void setWideMapCounterDao(GenericColumnFamilyDao<ID, Long> wideMapCounterDao)
	{
		this.wideMapCounterDao = wideMapCounterDao;
	}
}
