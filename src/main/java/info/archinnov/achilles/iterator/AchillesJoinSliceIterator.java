package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.dao.AbstractDao.DEFAULT_LENGTH;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.JoinEntityHelper;
import info.archinnov.achilles.entity.execution_context.SafeExecutionContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * AchillesJoinSliceIterator
 * 
 * @author DuyHai DOAN
 * 
 *         Modification of original version from Hector ColumnSliceIterator
 * 
 */
public class AchillesJoinSliceIterator<K, N extends AbstractComposite, V, JOIN_ID, KEY, VALUE>
		extends AbstractAchillesSliceIterator<N> implements Iterator<HColumn<N, VALUE>>
{

	private SliceQuery<K, N, V> query;
	private Iterator<HColumn<N, VALUE>> iterator;

	private PropertyMeta<KEY, VALUE> propertyMeta;
	private JoinEntityHelper joinHelper = new JoinEntityHelper();
	private GenericEntityDao<JOIN_ID> joinEntityDao;

	public AchillesJoinSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, //
			GenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, N, V> query, N start, //
			final N finish, boolean reversed)
	{
		this(policy, joinEntityDao, cf, propertyMeta, query, start, finish, reversed,
				DEFAULT_LENGTH);
	}

	public AchillesJoinSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, //
			GenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, N, V> query, N start, //
			final N finish, boolean reversed, int count)
	{
		this(policy, joinEntityDao, cf, propertyMeta, query, start, new ColumnSliceFinish<N>()
		{

			@Override
			public N function()
			{
				return finish;
			}
		}, reversed, count);
	}

	public AchillesJoinSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, //
			GenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, N, V> query, N start, //
			ColumnSliceFinish<N> finish, boolean reversed)
	{
		this(policy, joinEntityDao, cf, propertyMeta, query, start, finish, reversed,
				DEFAULT_LENGTH);
	}

	public AchillesJoinSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, //
			GenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, N, V> query, N start, //
			ColumnSliceFinish<N> finish, boolean reversed, int count)
	{
		super(policy, cf, start, finish, reversed, count);
		this.joinEntityDao = joinEntityDao;
		this.propertyMeta = propertyMeta;
		this.query = query;
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

		Iterator<HColumn<N, V>> iter = executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HColumn<N, V>>>()
		{
			@Override
			public Iterator<HColumn<N, V>> execute()
			{
				return query.execute().get().getColumns().iterator();
			}
		});

		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();
		Map<JOIN_ID, Pair<N, Integer>> hColumMap = new HashMap<JOIN_ID, Pair<N, Integer>>();

		while (iter.hasNext())
		{
			HColumn<N, V> hColumn = iter.next();

			PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
					.joinIdMeta();

			JOIN_ID joinId;
			if (propertyMeta.type().isExternal())
			{

				joinId = (JOIN_ID) joinIdMeta.castValue(hColumn.getValue());
			}
			else
			{
				joinId = (JOIN_ID) joinIdMeta.getValueFromString(hColumn.getValue());

			}
			joinIds.add(joinId);
			hColumMap.put(joinId, new Pair<N, Integer>(hColumn.getName(), hColumn.getTtl()));
		}
		List<HColumn<N, VALUE>> joinedHColumns = new ArrayList<HColumn<N, VALUE>>();

		if (joinIds.size() > 0)
		{

			Map<JOIN_ID, VALUE> loadedEntities = joinHelper.loadJoinEntities(
					propertyMeta.getValueClass(), joinIds,
					(EntityMeta<JOIN_ID>) propertyMeta.joinMeta(), joinEntityDao);

			for (JOIN_ID joinId : joinIds)
			{
				Pair<N, Integer> pair = hColumMap.get(joinId);
				N name = pair.left;
				Integer ttl = pair.right;

				HColumn<N, VALUE> joinedHColumn = new JoinHColumn<N, VALUE>();

				joinedHColumn.setName(name).setValue(loadedEntities.get(joinId)).setTtl(ttl);
				joinedHColumns.add(joinedHColumn);
			}
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
