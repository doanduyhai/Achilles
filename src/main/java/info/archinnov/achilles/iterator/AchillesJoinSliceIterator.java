package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.dao.ThriftAbstractDao.DEFAULT_LENGTH;
import static info.archinnov.achilles.iterator.AbstractAchillesSliceIterator.IteratorType.ACHILLES_JOIN_SLICE_ITERATOR;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftJoinEntityHelper;
import info.archinnov.achilles.entity.context.execution.SafeExecutionContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
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
public class AchillesJoinSliceIterator<K, V, JOIN_ID, KEY, VALUE> extends
		AbstractAchillesSliceIterator<HColumn<Composite, VALUE>>
{

	private SliceQuery<K, Composite, V> query;
	private PropertyMeta<KEY, VALUE> propertyMeta;
	private ThriftJoinEntityHelper joinHelper = new ThriftJoinEntityHelper();
	private ThriftGenericEntityDao<JOIN_ID> joinEntityDao;

	public AchillesJoinSliceIterator( //
			AchillesConsistencyLevelPolicy policy, //
			ThriftGenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, Composite, V> query, Composite start, //
			final Composite finish, boolean reversed)
	{
		this(policy, joinEntityDao, cf, propertyMeta, query, start, finish, reversed,
				DEFAULT_LENGTH);
	}

	public AchillesJoinSliceIterator(AchillesConsistencyLevelPolicy policy, //
			ThriftGenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, Composite, V> query, Composite start, //
			final Composite finish, boolean reversed, int count)
	{
		this(policy, joinEntityDao, cf, propertyMeta, query, start, new ColumnSliceFinish()
		{
			@Override
			public Composite function()
			{
				return finish;
			}
		}, reversed, count);
	}

	public AchillesJoinSliceIterator(AchillesConsistencyLevelPolicy policy, //
			ThriftGenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, Composite, V> query, Composite start, //
			ColumnSliceFinish finish, boolean reversed)
	{
		this(policy, joinEntityDao, cf, propertyMeta, query, start, finish, reversed,
				DEFAULT_LENGTH);
	}

	public AchillesJoinSliceIterator(AchillesConsistencyLevelPolicy policy, //
			ThriftGenericEntityDao<JOIN_ID> joinEntityDao, //
			String cf, PropertyMeta<KEY, VALUE> propertyMeta, //
			SliceQuery<K, Composite, V> query, Composite start, //
			ColumnSliceFinish finish, boolean reversed, int count)
	{
		super(policy, cf, start, finish, reversed, count);
		this.joinEntityDao = joinEntityDao;
		this.propertyMeta = propertyMeta;
		this.query = query;
		this.query.setRange(this.start, this.finish.function(), this.reversed, this.count);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Iterator<HColumn<Composite, VALUE>> fetchData()
	{

		Iterator<HColumn<Composite, V>> iter = executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HColumn<Composite, V>>>()
		{
			@Override
			public Iterator<HColumn<Composite, V>> execute()
			{
				return query.execute().get().getColumns().iterator();
			}
		});

		List<JOIN_ID> joinIds = new ArrayList<JOIN_ID>();
		Map<JOIN_ID, Pair<Composite, Integer>> hColumMap = new HashMap<JOIN_ID, Pair<Composite, Integer>>();

		while (iter.hasNext())
		{
			HColumn<Composite, V> hColumn = iter.next();

			PropertyMeta<Void, JOIN_ID> joinIdMeta = (PropertyMeta<Void, JOIN_ID>) propertyMeta
					.joinIdMeta();

			JOIN_ID joinId;
			if (propertyMeta.type().isWideMap())
			{
				joinId = (JOIN_ID) joinIdMeta.castValue(hColumn.getValue());
			}
			else
			{
				joinId = (JOIN_ID) joinIdMeta.getValueFromString(hColumn.getValue());

			}
			joinIds.add(joinId);
			hColumMap
					.put(joinId, new Pair<Composite, Integer>(hColumn.getName(), hColumn.getTtl()));
		}
		List<HColumn<Composite, VALUE>> joinedHColumns = new ArrayList<HColumn<Composite, VALUE>>();

		if (joinIds.size() > 0)
		{

			Map<JOIN_ID, VALUE> loadedEntities = joinHelper.loadJoinEntities(
					propertyMeta.getValueClass(), joinIds,
					(EntityMeta<JOIN_ID>) propertyMeta.joinMeta(), joinEntityDao);

			for (JOIN_ID joinId : joinIds)
			{
				Pair<Composite, Integer> pair = hColumMap.get(joinId);
				Composite name = pair.left;
				Integer ttl = pair.right;

				HColumn<Composite, VALUE> joinedHColumn = new JoinHColumn<Composite, VALUE>();

				joinedHColumn.setName(name).setValue(loadedEntities.get(joinId)).setTtl(ttl);
				joinedHColumns.add(joinedHColumn);
			}
		}
		return joinedHColumns.iterator();
	}

	@Override
	public HColumn<Composite, VALUE> next()
	{
		HColumn<Composite, VALUE> column = iterator.next();
		start = column.getName();
		columns++;

		return column;
	}

	@Override
	public void remove()
	{
		iterator.remove();
	}

	@Override
	protected void changeQueryRange()
	{
		query.setRange(start, finish.function(), reversed, count);
	}

	@Override
	protected void resetStartColumn(HColumn<Composite, VALUE> column)
	{
		start = column.getName();
	}

	@Override
	public IteratorType type()
	{
		return ACHILLES_JOIN_SLICE_ITERATOR;
	}
}
