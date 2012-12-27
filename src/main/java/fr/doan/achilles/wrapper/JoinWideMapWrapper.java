package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.entity.operations.EntityPersister;
import fr.doan.achilles.entity.operations.JoinEntityLoader;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.factory.IteratorFactory;
import fr.doan.achilles.validation.Validator;

/**
 * JoinWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapWrapper<ID, K, V> implements WideMap<K, V>
{

	private ID id;
	private GenericEntityDao<ID> dao;
	private PropertyMeta<K, V> joinWideMapMeta;

	private CompositeHelper helper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();
	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	private JoinEntityLoader joinEntityLoader = new JoinEntityLoader();
	private EntityPersister persister = new EntityPersister();
	private EntityLoader loader = new EntityLoader();

	private DynamicComposite buildComposite(K key)
	{
		return keyFactory.createForInsert(joinWideMapMeta, key);
	}

	@Override
	public V get(K key)
	{
		Object joinId = dao.getValue(id, buildComposite(key));

		return joinEntityLoader.loadJoinEntity(joinId, joinWideMapMeta);
	}

	@Override
	public void insert(K key, V value, int ttl)
	{
		Object joinId = persistOrChecKJoinEntity(value);
		dao.setValue(id, buildComposite(key), (Object) joinId, ttl);
	}

	@Override
	public void insert(K key, V value)
	{
		Object joinId = persistOrChecKJoinEntity(value);
		dao.setValue(id, buildComposite(key), (Object) joinId);
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

	@Override
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		helper.checkBounds(joinWideMapMeta, start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				joinWideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return keyValueFactory.createListForWideMap(joinWideMapMeta, hColumns);
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

		DynamicComposite[] queryComps = keyFactory.createForQuery( //
				joinWideMapMeta, start, inclusiveStart, end, inclusiveEnd, reverse);

		ColumnSliceIterator<ID, DynamicComposite, Object> columnSliceIterator = dao
				.getColumnsIterator(id, queryComps[0], queryComps[1], reverse, count);

		return iteratorFactory
				.createKeyValueIteratorForEntity(columnSliceIterator, joinWideMapMeta);
	}

	@Override
	public void remove(K key)
	{
		dao.removeColumn(id, buildComposite(key));
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

		helper.checkBounds(joinWideMapMeta, start, end, false);

		DynamicComposite[] queryComps = keyFactory.createForQuery(//
				joinWideMapMeta, start, inclusiveStart, end, inclusiveEnd, false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}

	private Object persistOrChecKJoinEntity(V value)
	{
		Object joinId = null;
		EntityMeta<?> targetEntityMeta = joinWideMapMeta.getJoinMetaHolder().getEntityMeta();
		String entityType = targetEntityMeta.getClass().getCanonicalName();

		try
		{
			joinId = targetEntityMeta.getIdMeta().getGetter().invoke(value);
		}
		catch (Exception e)
		{
			throw new IllegalStateException("Cannot access primary key of entity '" + entityType
					+ "'");
		}

		Validator.validateNotNull(joinId, "The primary key of entity '" + entityType
				+ "' should not be null");

		if (joinWideMapMeta.getJoinMetaHolder().isInsertable())
		{
			persister.persist(value, targetEntityMeta);
		}
		else
		{
			V refreshedFromDb = (V) loader.load(joinWideMapMeta.getValueClass(), joinId,
					(EntityMeta) targetEntityMeta);
			if (refreshedFromDb == null)
			{
				throw new IllegalStateException(
						"The entity '"
								+ entityType
								+ "' with id '"
								+ joinId
								+ "' cannot be found. Maybe you should persist it first or set 'insertable=true' on the @JoinColumn");
			}

		}
		return joinId;
	}

	public void setId(ID id)
	{
		this.id = id;
	}

	public void setDao(GenericEntityDao<ID> dao)
	{
		this.dao = dao;
	}

	public void setWideMapMeta(PropertyMeta<K, V> wideMapMeta)
	{
		this.joinWideMapMeta = wideMapMeta;
	}

}
