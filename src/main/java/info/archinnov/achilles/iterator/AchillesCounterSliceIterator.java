package info.archinnov.achilles.iterator;

/**
 * AchillesCounterSliceIterator
 *
 * @author DuyHai DOAN
 * 
 * Modification of original version from Hector ColumnSliceIterator
 *
 */

import static info.archinnov.achilles.dao.ThriftAbstractDao.DEFAULT_LENGTH;
import static info.archinnov.achilles.iterator.AbstractAchillesSliceIterator.IteratorType.ACHILLES_COUNTER_SLICE_ITERATOR;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.context.execution.SafeExecutionContext;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.SliceCounterQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AchillesCounterSliceIterator<K> extends
		AbstractAchillesSliceIterator<HCounterColumn<Composite>>
{
	private static final Logger log = LoggerFactory.getLogger(AchillesCounterSliceIterator.class);

	private SliceCounterQuery<K, Composite> query;

	public AchillesCounterSliceIterator(AchillesConsistencyLevelPolicy policy, String cf,
			SliceCounterQuery<K, Composite> query, Composite start, final Composite finish,
			boolean reversed)
	{
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesCounterSliceIterator(AchillesConsistencyLevelPolicy policy, String cf,
			SliceCounterQuery<K, Composite> query, Composite start, final Composite finish,
			boolean reversed, int count)
	{
		this(policy, cf, query, start, new ColumnSliceFinish()
		{

			@Override
			public Composite function()
			{
				return finish;
			}
		}, reversed, count);
	}

	public AchillesCounterSliceIterator(AchillesConsistencyLevelPolicy policy, String cf,
			SliceCounterQuery<K, Composite> query, Composite start, ColumnSliceFinish finish,
			boolean reversed)
	{
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesCounterSliceIterator(AchillesConsistencyLevelPolicy policy, String cf,
			SliceCounterQuery<K, Composite> query, Composite start, ColumnSliceFinish finish,
			boolean reversed, int count)
	{
		super(policy, cf, start, finish, reversed, count);
		this.query = query;
		this.query.setRange(this.start, this.finish.function(), this.reversed, this.count);
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

	@Override
	protected Iterator<HCounterColumn<Composite>> fetchData()
	{
		return executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HCounterColumn<Composite>>>()
		{
			@Override
			public Iterator<HCounterColumn<Composite>> execute()
			{
				log.trace("Fetching next {} counter columns", count);
				return query.execute().get().getColumns().iterator();
			}
		});
	}

	@Override
	protected void changeQueryRange()
	{
		query.setRange(start, finish.function(), reversed, count);
	}

	@Override
	protected void resetStartColumn(HCounterColumn<Composite> column)
	{
		start = column.getName();
	}

	@Override
	public IteratorType type()
	{
		return ACHILLES_COUNTER_SLICE_ITERATOR;
	}
}
