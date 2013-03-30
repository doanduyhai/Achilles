package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * WideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<ID, K, V>
{
	protected ID id;
	protected PropertyMeta<K, V> propertyMeta;
	protected EntityProxifier proxifier;
	protected GenericDynamicCompositeDao<ID> entityDao;
	protected AbstractDao<K, ? extends AbstractComposite, V> dao;
	protected CompositeHelper compositeHelper;
	protected KeyValueFactory keyValueFactory;
	protected IteratorFactory iteratorFactory;
	protected DynamicCompositeKeyFactory keyFactory;

	protected DynamicComposite buildComposite(K key)
	{
		return keyFactory.createForInsert(propertyMeta, key);
	}

	@Override
	public V get(K key)
	{
		Object value = entityDao.getValue(id, buildComposite(key));
		return propertyMeta.getValueFromString(value);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		entityDao.setValueBatch(id, buildComposite(key), propertyMeta.writeValueToString(value),
				ttl, interceptor.getMutator());
		context.flush();
	}

	@Override
	public void insert(K key, V value)
	{
		entityDao.setValueBatch(id, buildComposite(key), propertyMeta.writeValueToString(value),
				interceptor.getMutator());
		context.flush();
	}

	@Override
	public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());

		if (propertyMeta.isJoin())
		{
			return keyValueFactory.createJoinKeyValueListForDynamicComposite(context, propertyMeta,
					hColumns);
		}
		else
		{

			return keyValueFactory.createKeyValueListForDynamicComposite(context, propertyMeta,
					hColumns);
		}
	}

	@Override
	public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());
		if (propertyMeta.isJoin())
		{
			return keyValueFactory.createJoinValueListForDynamicComposite(context, propertyMeta,
					hColumns);
		}
		else
		{

			return keyValueFactory.createValueListForDynamicComposite(propertyMeta, hColumns);
		}
	}

	@Override
	public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, ordering);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		List<HColumn<DynamicComposite, String>> hColumns = entityDao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], count, ordering.isReverse());
		return keyValueFactory.createKeyListForDynamicComposite(propertyMeta, hColumns);
	}

	@Override
	public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds,
			OrderingMode ordering)
	{

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				propertyMeta, start, end, bounds, ordering);

		AchillesSliceIterator<ID, DynamicComposite, String> columnSliceIterator = entityDao
				.getColumnsIterator(id, queryComps[0], queryComps[1], ordering.isReverse(), count);

		return iteratorFactory.createKeyValueIteratorForDynamicComposite(context,
				columnSliceIterator, propertyMeta);
	}

	@Override
	public void remove(K key)
	{
		entityDao.removeColumnBatch(id, buildComposite(key), interceptor.getMutator());
		context.flush();
	}

	@Override
	public void remove(K start, K end, BoundingMode bounds)
	{

		compositeHelper.checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);

		DynamicComposite[] queryComps = keyFactory.createForQuery(//
				propertyMeta, start, end, bounds, OrderingMode.ASCENDING);

		entityDao
				.removeColumnRangeBatch(id, queryComps[0], queryComps[1], interceptor.getMutator());
		context.flush();
	}

	@Override
	public void removeFirst(int count)
	{
		entityDao.removeColumnRangeBatch(id, null, null, false, count, interceptor.getMutator());
		context.flush();
	}

	@Override
	public void removeLast(int count)
	{
		entityDao.removeColumnRangeBatch(id, null, null, true, count, interceptor.getMutator());
		context.flush();
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.propertyMeta = wideMapMeta;
	}

	public void setEntityProxifier(EntityProxifier proxifier)
	{
		this.proxifier = proxifier;
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

	public void setKeyFactory(DynamicCompositeKeyFactory keyFactory)
	{
		this.keyFactory = keyFactory;
	}

	public void setEntityDao(GenericDynamicCompositeDao<ID> entityDao)
	{
		this.entityDao = entityDao;
	}
}
