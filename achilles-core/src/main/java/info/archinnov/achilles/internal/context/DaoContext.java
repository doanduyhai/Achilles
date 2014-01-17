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
package info.archinnov.achilles.internal.context;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.*;
import static info.archinnov.achilles.internal.consistency.ConsistencyConverter.getCQLLevel;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.statement.StatementGenerator;
import info.archinnov.achilles.internal.statement.cache.CacheManager;
import info.archinnov.achilles.internal.statement.cache.StatementCacheKey;
import info.archinnov.achilles.internal.statement.prepared.PreparedStatementBinder;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;

public class DaoContext {
	private static final Logger log = LoggerFactory.getLogger(DaoContext.class);

	private Map<Class<?>, PreparedStatement> insertPSs;
	private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;
	private Map<Class<?>, PreparedStatement> selectPSs;
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;
	private Map<CQLQueryType, PreparedStatement> counterQueryMap;
	private Map<Class<?>, Map<CQLQueryType, Map<String, PreparedStatement>>> clusteredCounterQueryMap;
	private Session session;

	private PreparedStatementBinder binder = new PreparedStatementBinder();
	private CacheManager cacheManager = new CacheManager();
	private StatementGenerator statementGenerator = new StatementGenerator();
	private ConsistencyOverrider overrider = new ConsistencyOverrider();

	public DaoContext(Map<Class<?>, PreparedStatement> insertPSs,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache, Map<Class<?>, PreparedStatement> selectPSs,
			Map<Class<?>, Map<String, PreparedStatement>> removePSs,
			Map<CQLQueryType, PreparedStatement> counterQueryMap,
			Map<Class<?>, Map<CQLQueryType, Map<String, PreparedStatement>>> clusteredCounterQueryMap, Session session) {
		this.insertPSs = insertPSs;
		this.dynamicPSCache = dynamicPSCache;
		this.selectPSs = selectPSs;
		this.removePSs = removePSs;
		this.counterQueryMap = counterQueryMap;
		this.clusteredCounterQueryMap = clusteredCounterQueryMap;
		this.session = session;
	}

	public void pushInsertStatement(PersistenceContext context) {
		log.debug("Push insert statement for PersistenceContext '{}'", context);

		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Optional<Integer> ttlO = context.getTtt();
		Optional<Long> timestampO = context.getTimestamp();
		ConsistencyLevel writeLevel = overrider.getWriteLevel(context, entityMeta);
		if (timestampO.isPresent()) {
			final Pair<Insert, Object[]> pair = statementGenerator.generateInsert(context.getEntity(), entityMeta);
			Insert insert = pair.left;
			Object[] boundValues = pair.right;
			Insert.Options insertOptions = insert.using(timestamp(timestampO.get()));
			boundValues = ArrayUtils.add(boundValues, timestampO.get());

			if (ttlO.isPresent()) {
				insertOptions = insertOptions.and(ttl(ttlO.get()));
				boundValues = ArrayUtils.add(boundValues, ttlO.get());
			}
			context.pushStatement(new RegularStatementWrapper(insertOptions, boundValues, getCQLLevel(writeLevel)));
		} else {
			PreparedStatement ps = insertPSs.get(entityClass);
			BoundStatementWrapper bsWrapper = binder.bindForInsert(ps, entityMeta, context.getEntity(), writeLevel,
					ttlO);
			context.pushStatement(bsWrapper);
		}
	}

	public void pushUpdateStatement(PersistenceContext context, List<PropertyMeta> pms) {
		log.debug("Push insert statement for PersistenceContext '{}' and properties '{}'", context, pms);
		EntityMeta entityMeta = context.getEntityMeta();
		Optional<Integer> ttlO = context.getTtt();
		Optional<Long> timestampO = context.getTimestamp();
		ConsistencyLevel writeLevel = overrider.getWriteLevel(context, entityMeta);
		if (timestampO.isPresent()) {
			final Pair<Update.Where, Object[]> pair = statementGenerator.generateUpdateFields(context.getEntity(),
					entityMeta, pms);

			final Update.Where where = pair.left;
			Object[] boundValues = pair.right;
			Update.Options updateOptions = where.using(timestamp(timestampO.get()));
			boundValues = ArrayUtils.add(boundValues, timestampO.get());

			if (ttlO.isPresent()) {
				updateOptions = updateOptions.and(ttl(ttlO.get()));
				boundValues = ArrayUtils.add(boundValues, ttlO.get());
			}
			context.pushStatement(new RegularStatementWrapper(updateOptions, boundValues, getCQLLevel(writeLevel)));
		} else {
			PreparedStatement ps = cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms);
			BoundStatementWrapper bsWrapper = binder.bindForUpdate(ps, entityMeta, pms, context.getEntity(),
					writeLevel, ttlO);
			context.pushStatement(bsWrapper);
		}
	}

	public Row loadProperty(PersistenceContext context, PropertyMeta pm) {
		log.debug("Load property '{}' for PersistenceContext '{}'", pm, context);
		PreparedStatement ps = cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm);
		ConsistencyLevel readLevel = overrider.getReadLevel(context, pm);
		List<Row> rows = executeReadWithConsistency(context, ps, readLevel);
		return returnFirstRowOrNull(rows);
	}

	public void bindForRemoval(PersistenceContext context, String tableName) {
		log.debug("Push delete statement for PersistenceContext '{}'", context);
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Map<String, PreparedStatement> psMap = removePSs.get(entityClass);

		if (psMap.containsKey(tableName)) {
			ConsistencyLevel writeLevel = overrider.getWriteLevel(context, entityMeta);
			BoundStatementWrapper bsWrapper = binder.bindStatementWithOnlyPKInWhereClause(psMap.get(tableName),
					entityMeta, context.getPrimaryKey(), writeLevel);
			context.pushStatement(bsWrapper);
		} else {
			throw new AchillesException("Cannot find prepared statement for deletion for table '" + tableName + "'");
		}
	}

	// Simple counter
	public void bindForSimpleCounterIncrement(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Long increment) {
		log.debug("Push simple counter increment statement for PersistenceContext '{}' and value '{}'", context,
				increment);
		PreparedStatement ps = counterQueryMap.get(INCR);
		ConsistencyLevel writeLevel = overrider.getWriteLevel(context, counterMeta);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				context.getPrimaryKey(), increment, writeLevel);
		context.pushCounterStatement(bsWrapper);
	}

	public void incrementSimpleCounter(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Long increment, ConsistencyLevel consistencyLevel) {
		log.debug("Increment immediately simple counter for PersistenceContext '{}' and value '{}'", context, increment);
		PreparedStatement ps = counterQueryMap.get(INCR);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				context.getPrimaryKey(), increment, consistencyLevel);
		context.executeImmediate(bsWrapper);
	}

	public void decrementSimpleCounter(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Long decrement, ConsistencyLevel consistencyLevel) {
		log.debug("Decrement immediately simple counter for PersistenceContext '{}' and value '{}'", context, decrement);
		PreparedStatement ps = counterQueryMap.get(DECR);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				context.getPrimaryKey(), decrement, consistencyLevel);
		context.executeImmediate(bsWrapper);
	}

	public Row getSimpleCounter(PersistenceContext context, PropertyMeta counterMeta, ConsistencyLevel consistencyLevel) {
		log.debug("Get simple counter value for counterMeta '{}' PersistenceContext '{}' using Consistency level '{}'",
				counterMeta, context, consistencyLevel);
		PreparedStatement ps = counterQueryMap.get(SELECT);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterSelect(ps, context.getEntityMeta(), counterMeta,
				context.getPrimaryKey(), consistencyLevel);
		ResultSet resultSet = context.executeImmediate(bsWrapper);
		return returnFirstRowOrNull(resultSet.all());
	}

	public void bindForSimpleCounterDelete(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Object primaryKey) {
		log.debug("Push simple counter deletion statement for counterMeta '{}' and PersistenceContext '{}'",
				counterMeta, context);
		PreparedStatement ps = counterQueryMap.get(DELETE);
		ConsistencyLevel writeLevel = overrider.getWriteLevel(context, counterMeta);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterDelete(ps, meta, counterMeta, primaryKey,
				writeLevel);
		context.pushCounterStatement(bsWrapper);
	}

	// Clustered counter
	public void pushClusteredCounterIncrementStatement(PersistenceContext context, EntityMeta meta,
			PropertyMeta counterMeta, Long increment) {
		log.debug(
				"Push clustered counter increment statement for counterMeta '{}' and PersistenceContext '{}' and value '{}'",
				counterMeta, context, increment);
		ConsistencyLevel writeLevel = overrider.getWriteLevel(context, counterMeta);
		PreparedStatement ps = clusteredCounterQueryMap.get(meta.getEntityClass()).get(INCR)
				.get(counterMeta.getPropertyName());
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterIncrementDecrement(ps, meta,
				context.getPrimaryKey(), increment, writeLevel);
		context.pushCounterStatement(bsWrapper);
	}

	public Row getClusteredCounter(PersistenceContext context, ConsistencyLevel consistencyLevel) {
		log.debug("Get clustered counter for PersistenceContext '{}' and Consistency level '{}'", context,
				consistencyLevel);
		EntityMeta entityMeta = context.getEntityMeta();
		PreparedStatement ps = clusteredCounterQueryMap.get(entityMeta.getEntityClass()).get(SELECT)
				.get(SELECT_ALL.name());
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterSelect(ps, entityMeta, context.getPrimaryKey(),
				consistencyLevel);
		ResultSet resultSet = context.executeImmediate(bsWrapper);

		return returnFirstRowOrNull(resultSet.all());
	}

	public Long getClusteredCounterColumn(PersistenceContext context, PropertyMeta counterMeta,
			ConsistencyLevel consistencyLevel) {
		log.debug("Get clustered counter for PersistenceContext '{}' and Consistency level '{}'", context,
				consistencyLevel);
		EntityMeta entityMeta = context.getEntityMeta();
		final String counterColumnName = counterMeta.getPropertyName();
		PreparedStatement ps = clusteredCounterQueryMap.get(entityMeta.getEntityClass()).get(SELECT)
				.get(counterColumnName);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterSelect(ps, entityMeta, context.getPrimaryKey(),
				consistencyLevel);
		Row row = context.executeImmediate(bsWrapper).one();
		Long counterValue = null;
		if (row != null && !row.isNull(counterColumnName)) {
			counterValue = row.getLong(counterColumnName);
		}
		return counterValue;
	}

	public void bindForClusteredCounterDelete(PersistenceContext context, EntityMeta meta, Object primaryKey) {
		log.debug("Push clustered counter deletion statement for PersistenceContext '{}'", context);
		PreparedStatement ps = clusteredCounterQueryMap.get(meta.getEntityClass()).get(DELETE).get(DELETE_ALL.name());
		ConsistencyLevel writeLevel = overrider.getWriteLevel(context, meta);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterDelete(ps, meta, primaryKey, writeLevel);
		context.pushCounterStatement(bsWrapper);
	}

	public Row loadEntity(PersistenceContext context) {
		log.debug("Load entity for PersistenceContext '{}'", context);
		EntityMeta meta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectPSs.get(entityClass);

		ConsistencyLevel readLevel = overrider.getReadLevel(context, meta);
		List<Row> rows = executeReadWithConsistency(context, ps, readLevel);
		return returnFirstRowOrNull(rows);
	}

	private List<Row> executeReadWithConsistency(PersistenceContext context, PreparedStatement ps,
			ConsistencyLevel readLevel) {
		EntityMeta entityMeta = context.getEntityMeta();
		BoundStatementWrapper bsWrapper = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta,
				context.getPrimaryKey(), readLevel);
		return context.executeImmediate(bsWrapper).all();
	}

	private Row returnFirstRowOrNull(List<Row> rows) {
		if (rows.isEmpty()) {
			return null;
		} else {
			return rows.get(0);
		}
	}

	public ResultSet execute(AbstractStatementWrapper statementWrapper) {
		return statementWrapper.execute(session);
	}

	public PreparedStatement prepare(RegularStatement statement) {
		return session.prepare(statement.getQueryString());
	}

	public ResultSet bindAndExecute(PreparedStatement ps, Object... params) {
		BoundStatement bs = ps.bind(params);
		return new BoundStatementWrapper(bs, params, ps.getConsistencyLevel()).execute(session);
	}

	public void executeBatch(BatchStatement batch) {
		session.execute(batch);
	}

	public Session getSession() {
		return session;
	}

}
