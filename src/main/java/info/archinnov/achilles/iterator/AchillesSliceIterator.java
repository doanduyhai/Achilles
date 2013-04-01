package info.archinnov.achilles.iterator;

/**
 * AchillesSliceIterator
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
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.SliceQuery;

public class AchillesSliceIterator<K, N extends AbstractComposite, V> extends
		AbstractAchillesSliceIterator<N> implements Iterator<HColumn<N, V>>
{

	private SliceQuery<K, N, V> query;
	private Iterator<HColumn<N, V>> iterator;

	public AchillesSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, String cf,
			SliceQuery<K, N, V> query, N start, final N finish, boolean reversed)
	{
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, String cf,
			SliceQuery<K, N, V> query, N start, final N finish, boolean reversed, int count)
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

	public AchillesSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, String cf,
			SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed)
	{
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public AchillesSliceIterator(AchillesConfigurableConsistencyLevelPolicy policy, String cf,
			SliceQuery<K, N, V> query, N start, ColumnSliceFinish<N> finish, boolean reversed,
			int count)
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
	public HColumn<N, V> next()
	{
		HColumn<N, V> column = iterator.next();
		start = column.getName();
		columns++;

		return column;
	}

	@Override
	public void remove()
	{
		iterator.remove();
	}

	private Iterator<HColumn<N, V>> fetchData()
	{
		return executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HColumn<N, V>>>()
		{
			@Override
			public Iterator<HColumn<N, V>> execute()
			{
				return query.execute().get().getColumns().iterator();
			}
		});
	}
}
