package fr.doan.achilles.iterator;

import static fr.doan.achilles.dao.AbstractDao.DEFAULT_LENGTH;
import static fr.doan.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.service.ColumnSliceIterator.ColumnSliceFinish;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.Pair;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;

/**
 * JoinColumnSliceIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinColumnSliceIterator<K, N, V, KEY, VALUE> implements Iterator<HColumn<N, VALUE>>
{

	private SliceQuery<K, N, V> query;
	private Iterator<HColumn<N, VALUE>> iterator;
	private N start;
	private ColumnSliceFinish<N> finish;
	private boolean reversed;
	private int count = DEFAULT_LENGTH;
	private int columns = 0;
	private PropertyMeta<KEY, VALUE> propertyMeta;
	private EntityLoader loader = new EntityLoader();

	/**
	 * Constructor
	 * 
	 * @param query
	 *            Base SliceQuery to execute
	 * @param start
	 *            Starting point of the range
	 * @param finish
	 *            Finish point of the range.
	 * @param reversed
	 *            Whether or not the columns should be reversed
	 */
	public JoinColumnSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
			SliceQuery<K, N, V> query, N start, final N finish, boolean reversed)
	{
		this(propertyMeta, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	/**
	 * Constructor
	 * 
	 * @param query
	 *            Base SliceQuery to execute
	 * @param start
	 *            Starting point of the range
	 * @param finish
	 *            Finish point of the range.
	 * @param reversed
	 *            Whether or not the columns should be reversed
	 * @param count
	 *            the amount of columns to retrieve per batch
	 */
	public JoinColumnSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
			SliceQuery<K, N, V> query, N start, final N finish, boolean reversed, int count)
	{
		this(propertyMeta, query, start, new ColumnSliceFinish<N>()
		{

			@Override
			public N function()
			{
				return finish;
			}
		}, reversed, count);
	}

	/**
	 * Constructor
	 * 
	 * @param query
	 *            Base SliceQuery to execute
	 * @param start
	 *            Starting point of the range
	 * @param finish
	 *            Finish point of the range. Allows for a dynamically determined point
	 * @param reversed
	 *            Whether or not the columns should be reversed
	 */
	public JoinColumnSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
			SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed)
	{
		this(propertyMeta, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	/**
	 * Constructor
	 * 
	 * @param query
	 *            Base SliceQuery to execute
	 * @param start
	 *            Starting point of the range
	 * @param finish
	 *            Finish point of the range. Allows for a dynamically determined point
	 * @param reversed
	 *            Whether or not the columns should be reversed
	 * @param count
	 *            the amount of columns to retrieve per batch
	 */
	public JoinColumnSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
			SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed,
			int count)
	{
		this.propertyMeta = propertyMeta;
		this.query = query;
		this.start = start;
		this.finish = finish;
		this.reversed = reversed;
		this.count = count;
		this.query.setRange(this.start, this.finish.function(), this.reversed, this.count);
	}

	@Override
	public boolean hasNext()
	{
		if (iterator == null)
		{
			loadEntities();

		}
		else if (!iterator.hasNext() && columns == count)
		{ // only need to do another query if maximum columns were retrieved
			query.setRange(start, finish.function(), reversed, count);
			loadEntities();
			columns = 0;

			// First element is start which was the last element on the previous query result - skip it
			if (iterator.hasNext())
			{
				next();
			}
		}

		return iterator.hasNext();
	}

	@SuppressWarnings("unchecked")
	private void loadEntities()
	{
		Iterator<HColumn<N, V>> iter = query.execute().get().getColumns().iterator();
		int i = 0;
		List<V> joinIds = new ArrayList<V>();
		Map<V, Pair<N, Integer>> hColumMap = new HashMap<V, Pair<N, Integer>>();
		Serializer<?> nameSerializer;
		if (propertyMeta.getExternalWideMapProperties() != null)
		{
			nameSerializer = COMPOSITE_SRZ;
		}
		else
		{
			nameSerializer = DYNA_COMP_SRZ;
		}

		while (iter.hasNext() && i < count)
		{
			HColumn<N, V> hColumn = iter.next();
			joinIds.add(hColumn.getValue());
			hColumMap.put(hColumn.getValue(),
					new Pair<N, Integer>(hColumn.getName(), hColumn.getTtl()));
			i++;
		}
		Map<V, VALUE> loadedEntities = loader.loadJoinEntities(propertyMeta.getValueClass(),
				joinIds, propertyMeta.getJoinProperties().getEntityMeta());

		List<HColumn<N, VALUE>> joinedHColumns = new ArrayList<HColumn<N, VALUE>>();
		for (V joinId : joinIds)
		{
			Pair<N, Integer> pair = hColumMap.get(joinId);
			N name = pair.left;
			Integer ttl = pair.right;

			HColumn<N, VALUE> joinedHColumn = HFactory.createColumn(name,
					loadedEntities.get(joinId), ttl, (Serializer<N>) nameSerializer,
					propertyMeta.getValueSerializer());

			joinedHColumns.add(joinedHColumn);
		}

		iterator = joinedHColumns.iterator();
	}

	@Override
	public HColumn<N, VALUE> next()
	{
		HColumn<N, VALUE> column = iterator.next();
		start = column.getName();
		columns++;

		return column;
	}

	@Override
	public void remove()
	{
		iterator.remove();
	}
}
