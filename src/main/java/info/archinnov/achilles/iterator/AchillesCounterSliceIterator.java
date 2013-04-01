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
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.execution_context.SafeExecutionContext;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.query.SliceCounterQuery;

public class AchillesCounterSliceIterator<K, N extends AbstractComposite> extends
		AbstractAchillesSliceIterator<N> implements Iterator<HCounterColumn<N>>
{

	private SliceCounterQuery<K, N> query;
	private Iterator<HCounterColumn<N>> iterator;

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
		super(policy, cf, start, finish, reversed, count);
		this.query = query;
		this.query.setRange(this.start, this.finish.function(), this.reversed, this.count);
	}

	@Override
	public boolean hasNext()
	{
		if (iterator == null)
		{
			iterator = fetchData();
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
			iterator = fetchData();
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

	private Iterator<HCounterColumn<N>> fetchData()
	{
		return executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HCounterColumn<N>>>()
		{
			@Override
			public Iterator<HCounterColumn<N>> execute()
			{
				return query.execute().get().getColumns().iterator();
			}
		});
	}

}
