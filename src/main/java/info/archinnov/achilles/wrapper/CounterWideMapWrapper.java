package info.archinnov.achilles.wrapper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesCounterSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * CounterWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWideMapWrapper<ID, K> extends AbstractWideMapWrapper<ID, K, Long>
{

	protected ID id;
	protected String fqcn;
	protected PropertyMeta<Void, ID> idMeta;
	protected CounterDao counterDao;
	protected PropertyMeta<K, Long> propertyMeta;

	protected CompositeHelper compositeHelper;
	protected KeyValueFactory keyValueFactory;
	protected IteratorFactory iteratorFactory;
	protected CompositeKeyFactory compositeKeyFactory;
	protected DynamicCompositeKeyFactory dynamicCompositeKeyFactory;

	@Override
	public Long get(K key)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta);
		DynamicComposite comp = dynamicCompositeKeyFactory.createForQuery(propertyMeta, key, EQUAL);
		return counterDao.getCounterValue(keyComp, comp);

	}

	@Override
	public void insert(K key, Long value, int ttl)
	{
		throw new UnsupportedOperationException("Cannot insert counter value with ttl");
	}

	@Override
	public void insert(K key, Long value)
	{

		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta);
		DynamicComposite comp = dynamicCompositeKeyFactory.createForInsert(propertyMeta, key);

		counterDao.insertCounter(keyComp, comp, value,
				(Mutator<Composite>) interceptor.getCounterMutator());
		context.flush();
	}

	@Override
	public List<KeyValue<K, Long>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);
		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta);

		DynamicComposite[] queryComps = dynamicCompositeKeyFactory.createForQuery(propertyMeta,
				start, end, bounds, ordering);

		List<HCounterColumn<DynamicComposite>> hColumns = counterDao.findCounterColumnsRange(
				keyComp, queryComps[0], queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createCounterKeyValueListForDynamicComposite(propertyMeta, hColumns);
	}

	@Override
	public List<Long> findValues(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, ordering);
		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta);

		DynamicComposite[] queryComps = dynamicCompositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HCounterColumn<DynamicComposite>> hColumns = counterDao.findCounterColumnsRange(
				keyComp, queryComps[0], queryComps[1], count, ordering.isReverse());

		return keyValueFactory.createCounterValueListForDynamicComposite(propertyMeta, hColumns);
	}

	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{
		compositeHelper.checkBounds(propertyMeta, start, end, ordering);
		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta);
		DynamicComposite[] queryComps = dynamicCompositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HCounterColumn<DynamicComposite>> hColumns = counterDao.findCounterColumnsRange(
				keyComp, queryComps[0], queryComps[1], count, ordering.isReverse());
		return keyValueFactory.createCounterKeyListForDynamicComposite(propertyMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, Long> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{
		Composite keyComp = compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta);
		DynamicComposite[] queryComps = dynamicCompositeKeyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		AchillesCounterSliceIterator<Composite, DynamicComposite> columnSliceIterator = counterDao
				.getCounterColumnsIterator(keyComp, queryComps[0], queryComps[1],
						ordering.isReverse(), count);

		return iteratorFactory.createCounterKeyValueIteratorForDynamicComposite(
				columnSliceIterator, propertyMeta);
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

	public void setDynamicCompositeKeyFactory(DynamicCompositeKeyFactory keyFactory)
	{
		this.dynamicCompositeKeyFactory = keyFactory;
	}

	public void setCompositeKeyFactory(CompositeKeyFactory compositeKeyFactory)
	{
		this.compositeKeyFactory = compositeKeyFactory;
	}

	public void setFqcn(String fqcn)
	{
		this.fqcn = fqcn;
	}

	public void setIdMeta(PropertyMeta<Void, ID> idMeta)
	{
		this.idMeta = idMeta;
	}

	public void setCounterDao(CounterDao counterDao)
	{
		this.counterDao = counterDao;
	}
}
