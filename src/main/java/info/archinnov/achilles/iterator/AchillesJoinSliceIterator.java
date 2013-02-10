package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.dao.AbstractDao.DEFAULT_LENGTH;
import static info.archinnov.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.service.ColumnSliceIterator.ColumnSliceFinish;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.Pair;

/**
 * AchillesJoinSliceIterator
 * 
 * @author DuyHai DOAN
 * 
 *         Modification of original version from Hector ColumnSliceIterator
 * 
 */
public class AchillesJoinSliceIterator<K, N extends AbstractComposite, V, KEY, VALUE> implements
		Iterator<HColumn<N, VALUE>>
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

	public AchillesJoinSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
			SliceQuery<K, N, V> query, N start, final N finish, boolean reversed)
	{
		this(propertyMeta, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesJoinSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
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

	public AchillesJoinSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
			SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed)
	{
		this(propertyMeta, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesJoinSliceIterator(PropertyMeta<KEY, VALUE> propertyMeta,
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

			// Exclude start from the query because is has been already fetched
			if (reversed)
			{
				start.setEquality(ComponentEquality.LESS_THAN_EQUAL);
			}
			else
			{
				start.setEquality(ComponentEquality.GREATER_THAN_EQUAL);
			}

			query.setRange(start, finish.function(), reversed, count);
			loadEntities();
			columns = 0;
		}

		return iterator.hasNext();
	}

	@SuppressWarnings("unchecked")
	private void loadEntities()
	{
		Iterator<HColumn<N, V>> iter = query.execute().get().getColumns().iterator();
		List<V> joinIds = new ArrayList<V>();
		Map<V, Pair<N, Integer>> hColumMap = new HashMap<V, Pair<N, Integer>>();
		Serializer<?> nameSerializer;
		if (propertyMeta.type().isExternal())
		{
			nameSerializer = COMPOSITE_SRZ;
		}
		else
		{
			nameSerializer = DYNA_COMP_SRZ;
		}

		while (iter.hasNext())
		{
			HColumn<N, V> hColumn = iter.next();

			PropertyMeta<Void, ?> joinIdMeta = propertyMeta.getJoinProperties().getEntityMeta()
					.getIdMeta();

			V joinId;
			if (propertyMeta.type().isExternal())
			{

				joinId = (V) joinIdMeta.castValue(hColumn.getValue());
			}
			else
			{
				joinId = (V) joinIdMeta.getValueFromString(hColumn.getValue());

			}
			joinIds.add(joinId);
			hColumMap.put(joinId, new Pair<N, Integer>(hColumn.getName(), hColumn.getTtl()));
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
