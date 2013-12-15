/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.iterator.SliceQueryIterator;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.StatementGenerator;
import info.archinnov.achilles.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SliceQueryExecutor {

    private static final Logger log  = LoggerFactory.getLogger(SliceQueryExecutor.class);

    private StatementGenerator generator = new StatementGenerator();
	private EntityMapper mapper = new EntityMapper();
	private EntityProxifier proxifier = new EntityProxifier();
	private PersistenceContextFactory contextFactory;
	private DaoContext daoContext;
	private ConsistencyLevel defaultReadLevel;

	public SliceQueryExecutor(PersistenceContextFactory contextFactory, ConfigurationContext configContext,
                              DaoContext daoContext) {
		this.contextFactory = contextFactory;
		this.daoContext = daoContext;
		this.defaultReadLevel = configContext.getDefaultReadConsistencyLevel();
	}

	public <T> List<T> get(SliceQuery<T> sliceQuery) {
        log.debug("Get slice query");
		EntityMeta meta = sliceQuery.getMeta();

		List<T> clusteredEntities = new ArrayList();

		CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery(sliceQuery, defaultReadLevel);
        RegularStatementWrapper statementWrapper = generator.generateSelectSliceQuery(cqlSliceQuery, cqlSliceQuery.getLimit(),cqlSliceQuery.getBatchSize());
		List<Row> rows = daoContext.execute(statementWrapper).all();

		for (Row row : rows) {
			T clusteredEntity = meta.instanciate();
			mapper.setEagerPropertiesToEntity(row, meta, clusteredEntity);
			clusteredEntities.add(clusteredEntity);
		}

		return Lists.transform(clusteredEntities, this.<T>getProxyTransformer(meta.getEagerGetters()));
	}

	public <T> Iterator<T> iterator(SliceQuery<T> sliceQuery) {
        log.debug("Get iterator for slice query");
		CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery(sliceQuery, defaultReadLevel);
        RegularStatementWrapper statementWrapper = generator.generateSelectSliceQuery(cqlSliceQuery, cqlSliceQuery.getLimit(),cqlSliceQuery.getBatchSize());
		Iterator<Row> iterator = daoContext.execute(statementWrapper).iterator();
		PersistenceContext context = buildContextForQuery(sliceQuery);
		return new SliceQueryIterator(cqlSliceQuery, context, iterator);
	}

	public <T> void remove(SliceQuery<T> sliceQuery) {
        log.debug("Slice remove");
		CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery(sliceQuery, defaultReadLevel);
		cqlSliceQuery.validateSliceQueryForRemove();
        final RegularStatementWrapper statementWrapper = generator.generateRemoveSliceQuery(cqlSliceQuery);
        daoContext.execute(statementWrapper);
	}

	protected <T> PersistenceContext buildContextForQuery(SliceQuery<T> sliceQuery) {
        log.trace("Build PersistenceContext for slice query");
		ConsistencyLevel cl = sliceQuery.getConsistencyLevel() == null ? defaultReadLevel : sliceQuery
				.getConsistencyLevel();
		return contextFactory.newContextForSliceQuery(sliceQuery.getEntityClass(), sliceQuery.getPartitionComponents(),
				cl);
	}

	private <T> Function<T, T> getProxyTransformer(final List<Method> getters) {
		return new Function<T, T>() {
			@Override
			public T apply(T clusteredEntity) {
				PersistenceContext context = contextFactory.newContext(clusteredEntity);
				return proxifier.buildProxy(clusteredEntity, context, Sets.newHashSet(getters));
			}
		};
	}
}
