package info.archinnov.achilles.iterator;

/**
 * AchillesCounterSliceIterator
 *
 * @author DuyHai DOAN
 * 
 * Modification of original version from Hector ColumnSliceIterator
 *
 */

import static info.archinnov.achilles.dao.AbstractDao.DEFAULT_LENGTH;
import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.SliceCounterQuery;

public class AchillesCounterSliceIterator<K, N extends AbstractComposite> implements
		Iterator<HCounterColumn<N>>
{

	private SliceCounterQuery<K, N> query;
	private Iterator<HCounterColumn<N>> iterator;
	private N start;
	private ColumnSliceFinish<N> finish;
	private boolean reversed;
	private int count = DEFAULT_LENGTH;
	private int columns = 0;
	private AchillesConfigurableConsistencyLevelPolicy policy;
	private String columnFamily;

	public AchillesCounterSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy,
			String cf, SliceCounterQuery<K, N> query, N start, final N finish, boolean reversed)
	{
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesCounterSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy,
			String cf, SliceCounterQuery<K, N> query, N start, final N finish, boolean reversed,
			int count)
	{
		this(policy, cf, query, start, new ColumnSliceFinish<N>()
		{

			@Override
			public N function()
			{
				return finish;
			}
		}, reversed, count);
	}

	public AchillesCounterSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy,
			String cf, SliceCounterQuery<K, N> query, N start, ColumnSliceFinish<N> finish,
			boolean reversed)
	{
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesCounterSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy,
			String cf, SliceCounterQuery<K, N> query, N start, ColumnSliceFinish<N> finish,
			boolean reversed, int count)
	{
		this.policy = policy;
		this.columnFamily = cf;
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
			policy.loadConsistencyLevelForRead(columnFamily);
			iterator = query.execute().get().getColumns().iterator();
			policy.reinitDefaultConsistencyLevel();
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
			policy.loadConsistencyLevelForRead(columnFamily);
			iterator = query.execute().get().getColumns().iterator();
			policy.reinitDefaultConsistencyLevel();
			columns = 0;
		}

		return iterator.hasNext();
	}

	@Override
	public HCounterColumn<N> next()
	{
		HCounterColumn<N> column = iterator.next();
		start = column.getName();
		columns++;

		return column;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException(
				"Cannot remove counter value. Please set a its value to 0 instead of removing it");
	}

	public interface ColumnSliceFinish<N>
	{
		N function();
	}
}
