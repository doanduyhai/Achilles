package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * ThriftQueryExecutorImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftQueryExecutorImpl
{
	private ThriftCompositeFactory compositeFactory = new ThriftCompositeFactory();

	public <T> List<HColumn<Composite, Object>> findColumns(final SliceQuery<T> query,
			ThriftPersistenceContext context)
	{
		EntityMeta meta = query.getMeta();
		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();

		final Composite[] composites = compositeFactory.createForClusteredQuery(idMeta,
				query.getClusteringsFrom(),
				query.getClusteringsTo(),
				query.getBounding(), query.getOrdering());

		return context.executeWithReadConsistencyLevel(
				new SafeExecutionContext<List<HColumn<Composite, Object>>>()
				{
					@Override
					public List<HColumn<Composite, Object>> execute()
					{
						return wideRowDao.findRawColumnsRange(
								query.getPartitionKey(), composites[0], composites[1],
								query.getLimit(), query.getOrdering().isReverse());
					}
				}, query.getConsistencyLevel());
	}

	public <T> ThriftSliceIterator<Object, Object> getColumnsIterator(
			final SliceQuery<T> query,
			ThriftPersistenceContext context)
	{
		EntityMeta meta = query.getMeta();
		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();

		final Composite[] composites = compositeFactory.createForClusteredQuery(idMeta,
				query.getClusteringsFrom(),
				query.getClusteringsTo(),
				query.getBounding(), query.getOrdering());

		return context.executeWithReadConsistencyLevel(
				new SafeExecutionContext<ThriftSliceIterator<Object, Object>>()
				{
					@Override
					public ThriftSliceIterator<Object, Object> execute()
					{
						return wideRowDao.getColumnsIterator(
								query.getPartitionKey(), composites[0], composites[1],
								query.getOrdering().isReverse(), query.getBatchSize());
					}
				}, query.getConsistencyLevel());
	}

	public <T> ThriftJoinSliceIterator<Object, Object, Object> getJoinColumnsIterator(
			final SliceQuery<T> query,
			ThriftPersistenceContext context)
	{
		EntityMeta meta = query.getMeta();
		final PropertyMeta<?, ?> idMeta = meta.getIdMeta();
		final PropertyMeta<Object, Object> pm = (PropertyMeta<Object, Object>) meta.getFirstMeta();

		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();

		final ThriftGenericEntityDao joinEntityDao = context.findEntityDao(pm
				.joinMeta().getTableName());

		final Composite[] composites = compositeFactory.createForClusteredQuery(idMeta,
				query.getClusteringsFrom(),
				query.getClusteringsTo(),
				query.getBounding(), query.getOrdering());

		return context.executeWithReadConsistencyLevel(
				new SafeExecutionContext<ThriftJoinSliceIterator<Object, Object, Object>>()
				{
					@Override
					public ThriftJoinSliceIterator<Object, Object, Object> execute()
					{
						return wideRowDao.getJoinColumnsIterator(joinEntityDao, pm,
								query.getPartitionKey(), composites[0], composites[1],
								query.getOrdering().isReverse(), query.getBatchSize());
					}
				}, query.getConsistencyLevel());
	}

	public void removeColumns(List<HColumn<Composite, Object>> columns,
			final ConsistencyLevel consistencyLevel,
			final ThriftPersistenceContext context)
	{
		Object partitionKey = context.getPartitionKey();
		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();
		final Mutator<Object> mutator = wideRowDao.buildMutator();
		for (HColumn<Composite, Object> column : columns)
		{
			wideRowDao.removeColumnBatch(partitionKey, column.getName(), mutator);
		}

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				wideRowDao.executeMutator(mutator);
				return null;
			}
		}, consistencyLevel);

	}

	public <T> List<HCounterColumn<Composite>> findCounterColumns(final SliceQuery<T> query,
			ThriftPersistenceContext context)
	{
		EntityMeta meta = query.getMeta();
		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();

		final Composite[] composites = compositeFactory.createForClusteredQuery(idMeta,
				query.getClusteringsFrom(),
				query.getClusteringsTo(),
				query.getBounding(), query.getOrdering());

		return context.executeWithReadConsistencyLevel(
				new SafeExecutionContext<List<HCounterColumn<Composite>>>()
				{
					@Override
					public List<HCounterColumn<Composite>> execute()
					{
						return wideRowDao.findCounterColumnsRange(
								query.getPartitionKey(), composites[0], composites[1],
								query.getLimit(), query.getOrdering().isReverse());
					}
				}, query.getConsistencyLevel());
	}

	public <T> ThriftCounterSliceIterator<Object> getCounterColumnsIterator(
			final SliceQuery<T> query,
			ThriftPersistenceContext context)
	{
		EntityMeta meta = query.getMeta();
		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();

		final Composite[] composites = compositeFactory.createForClusteredQuery(idMeta,
				query.getClusteringsFrom(),
				query.getClusteringsTo(),
				query.getBounding(), query.getOrdering());

		return context.executeWithReadConsistencyLevel(
				new SafeExecutionContext<ThriftCounterSliceIterator<Object>>()
				{
					@Override
					public ThriftCounterSliceIterator<Object> execute()
					{
						return wideRowDao.getCounterColumnsIterator(
								query.getPartitionKey(), composites[0], composites[1],
								query.getOrdering().isReverse(), query.getBatchSize());
					}
				}, query.getConsistencyLevel());
	}

	public void removeCounterColumns(List<HCounterColumn<Composite>> counterColumns,
			final ConsistencyLevel consistencyLevel,
			final ThriftPersistenceContext context)
	{
		Object partitionKey = context.getPartitionKey();
		final ThriftGenericWideRowDao wideRowDao = context.getWideRowDao();
		final Mutator<Object> mutator = wideRowDao.buildMutator();
		for (HCounterColumn<Composite> counterColumn : counterColumns)
		{
			wideRowDao.removeCounterBatch(partitionKey, counterColumn.getName(), mutator);
		}

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				wideRowDao.executeMutator(mutator);
				return null;
			}
		}, consistencyLevel);

	}

}
