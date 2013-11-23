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
package info.archinnov.achilles.context;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.statement.StatementGenerator;
import info.archinnov.achilles.statement.cache.CacheManager;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.statement.prepared.PreparedStatementBinder;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;

public class DaoContext {
	public static final String ACHILLES_DML_STATEMENT = "ACHILLES_DML_STATEMENT";

	private static final Logger dmlLogger = LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);

	private Map<Class<?>, PreparedStatement> insertPSs;
	private Cache<StatementCacheKey, PreparedStatement> dynamicPSCache;
	private Map<Class<?>, PreparedStatement> selectEagerPSs;
	private Map<Class<?>, Map<String, PreparedStatement>> removePSs;
	private Map<CQLQueryType, PreparedStatement> counterQueryMap;
	private Map<Class<?>, Map<CQLQueryType, PreparedStatement>> clusteredCounterQueryMap;
	private Session session;

	private PreparedStatementBinder binder = new PreparedStatementBinder();
	private CacheManager cacheManager = new CacheManager();
	private StatementGenerator statementGenerator = new StatementGenerator();

	public DaoContext(Map<Class<?>, PreparedStatement> insertPSs,
                      Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
                      Map<Class<?>, PreparedStatement> selectEagerPSs, Map<Class<?>, Map<String,
            PreparedStatement>> removePSs,
                      Map<CQLQueryType, PreparedStatement> counterQueryMap,
                      Map<Class<?>, Map<CQLQueryType, PreparedStatement>> clusteredCounterQueryMap, Session session) {
		this.insertPSs = insertPSs;
		this.dynamicPSCache = dynamicPSCache;
		this.selectEagerPSs = selectEagerPSs;
		this.removePSs = removePSs;
		this.counterQueryMap = counterQueryMap;
		this.clusteredCounterQueryMap = clusteredCounterQueryMap;
		this.session = session;
	}

	public void pushInsertStatement(PersistenceContext context) {
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Optional<Integer> ttlO = context.getTtt();
		Optional<Long> timestampO = context.getTimestamp();
		ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, entityMeta);
		if (ttlO.isPresent() || timestampO.isPresent()) {
			Insert insert = statementGenerator.generateInsert(context.getEntity(), entityMeta);
			Insert.Options options = null;

			if (ttlO.isPresent() && timestampO.isPresent())
				options = insert.using(ttl(ttlO.get())).and(timestamp(timestampO.get()));
			else if (ttlO.isPresent())
				options = insert.using(ttl(ttlO.get()));
			else if (timestampO.isPresent())
				options = insert.using(timestamp(timestampO.get()));

			context.pushStatement(options, writeLevel);
		} else {
			PreparedStatement ps = insertPSs.get(entityClass);
			BoundStatementWrapper bsWrapper = binder.bindForInsert(ps, entityMeta, context.getEntity());
			context.pushBoundStatement(bsWrapper, writeLevel);
		}
	}

	public void pushUpdateStatement(PersistenceContext context, List<PropertyMeta> pms) {
		EntityMeta entityMeta = context.getEntityMeta();
		Optional<Integer> ttlO = context.getTtt();
		Optional<Long> timestampO = context.getTimestamp();
		ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, entityMeta);
		if (ttlO.isPresent() || timestampO.isPresent()) {
			Assignments update = statementGenerator.generateUpdateFields(context.getEntity(), entityMeta, pms);
			Update.Options options = null;

			if (ttlO.isPresent() && timestampO.isPresent())
				options = update.using(ttl(ttlO.get())).and(timestamp(timestampO.get()));
			else if (ttlO.isPresent())
				options = update.using(ttl(ttlO.get()));
			else if (timestampO.isPresent())
				options = update.using(timestamp(timestampO.get()));

			context.pushStatement(options, writeLevel);
		} else {
			PreparedStatement ps = cacheManager.getCacheForFieldsUpdate(session, dynamicPSCache, context, pms);
			BoundStatementWrapper bsWrapper = binder.bindForUpdate(ps, entityMeta, pms, context.getEntity());
			context.pushBoundStatement(bsWrapper, writeLevel);
		}
	}

	public Row loadProperty(PersistenceContext context, PropertyMeta pm) {
		PreparedStatement ps = cacheManager.getCacheForFieldSelect(session, dynamicPSCache, context, pm);
		ConsistencyLevel readLevel = getReadConsistencyLevel(context, pm);
		List<Row> rows = executeReadWithConsistency(context, ps, readLevel);
		return returnFirstRowOrNull(rows);
	}

	public void bindForRemoval(PersistenceContext context, String tableName) {
		EntityMeta entityMeta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		Map<String, PreparedStatement> psMap = removePSs.get(entityClass);

		if (psMap.containsKey(tableName)) {
			BoundStatementWrapper bsWrapper = binder.bindStatementWithOnlyPKInWhereClause(psMap.get(tableName),
					entityMeta, context.getPrimaryKey());
			ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, entityMeta);
			context.pushBoundStatement(bsWrapper, writeLevel);
		} else {
			throw new AchillesException("Cannot find prepared statement for deletion for table '" + tableName + "'");
		}
	}

	// Simple counter
	public void bindForSimpleCounterIncrement(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Long increment) {
		PreparedStatement ps = counterQueryMap.get(INCR);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				context.getPrimaryKey(), increment);

		ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, counterMeta);
		context.pushBoundStatement(bsWrapper, writeLevel);
	}

	public void incrementSimpleCounter(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Long increment, ConsistencyLevel consistencyLevel) {
		PreparedStatement ps = counterQueryMap.get(INCR);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				context.getPrimaryKey(), increment);
		context.executeImmediateWithConsistency(bsWrapper, consistencyLevel);
	}

	public void decrementSimpleCounter(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Long decrement, ConsistencyLevel consistencyLevel) {
		PreparedStatement ps = counterQueryMap.get(DECR);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta,
				context.getPrimaryKey(), decrement);
		context.executeImmediateWithConsistency(bsWrapper, consistencyLevel);
	}

	public Row getSimpleCounter(PersistenceContext context, PropertyMeta counterMeta,
			ConsistencyLevel consistencyLevel) {
		PreparedStatement ps = counterQueryMap.get(SELECT);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterSelect(ps, context.getEntityMeta(), counterMeta,
				context.getPrimaryKey());
		ResultSet resultSet = context.executeImmediateWithConsistency(bsWrapper, consistencyLevel);

		return returnFirstRowOrNull(resultSet.all());
	}

	public void bindForSimpleCounterDelete(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Object primaryKey) {
		PreparedStatement ps = counterQueryMap.get(DELETE);
		BoundStatementWrapper bsWrapper = binder.bindForSimpleCounterDelete(ps, meta, counterMeta, primaryKey);

		ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, counterMeta);
		context.pushBoundStatement(bsWrapper, writeLevel);
	}

	// Clustered counter
	public void pushClusteredCounterIncrementStatement(PersistenceContext context, EntityMeta meta,
			PropertyMeta counterMeta, Long increment) {
		ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, counterMeta);
		PreparedStatement ps = clusteredCounterQueryMap.get(meta.getEntityClass()).get(INCR);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterIncrementDecrement(ps, meta,
                                                                                           context.getPrimaryKey(), increment);
		context.pushBoundStatement(bsWrapper, writeLevel);
	}

	public void incrementClusteredCounter(PersistenceContext context, EntityMeta meta,
                                          Long increment, ConsistencyLevel consistencyLevel) {
		PreparedStatement ps = clusteredCounterQueryMap.get(meta.getEntityClass()).get(INCR);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterIncrementDecrement(ps, meta,
                                                                                           context.getPrimaryKey(), increment);
		context.executeImmediateWithConsistency(bsWrapper, consistencyLevel);
	}

	public void decrementClusteredCounter(PersistenceContext context, EntityMeta meta,
                                          Long decrement, ConsistencyLevel consistencyLevel) {
		PreparedStatement ps = clusteredCounterQueryMap.get(meta.getEntityClass()).get(DECR);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterIncrementDecrement(ps, meta,
                                                                                           context.getPrimaryKey(), decrement);
		context.executeImmediateWithConsistency(bsWrapper, consistencyLevel);
	}

	public Row getClusteredCounter(PersistenceContext context,
                                   ConsistencyLevel consistencyLevel) {
		EntityMeta entityMeta = context.getEntityMeta();
		PreparedStatement ps = clusteredCounterQueryMap.get(entityMeta.getEntityClass()).get(SELECT);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterSelect(ps, entityMeta,
                                                                               context.getPrimaryKey());
		ResultSet resultSet = context.executeImmediateWithConsistency(bsWrapper, consistencyLevel);

		return returnFirstRowOrNull(resultSet.all());
	}

	public void bindForClusteredCounterDelete(PersistenceContext context, EntityMeta meta, PropertyMeta counterMeta,
			Object primaryKey) {
		PreparedStatement ps = clusteredCounterQueryMap.get(meta.getEntityClass()).get(DELETE);
		BoundStatementWrapper bsWrapper = binder.bindForClusteredCounterDelete(ps, meta, primaryKey);
		ConsistencyLevel writeLevel = getWriteConsistencyLevel(context, counterMeta);
		context.pushBoundStatement(bsWrapper, writeLevel);
	}

	public Row eagerLoadEntity(PersistenceContext context) {
		EntityMeta meta = context.getEntityMeta();
		Class<?> entityClass = context.getEntityClass();
		PreparedStatement ps = selectEagerPSs.get(entityClass);

		ConsistencyLevel readLevel = getReadConsistencyLevel(context, meta);
		List<Row> rows = executeReadWithConsistency(context, ps, readLevel);
		return returnFirstRowOrNull(rows);
	}

	private List<Row> executeReadWithConsistency(PersistenceContext context, PreparedStatement ps,
			ConsistencyLevel readLevel) {
		EntityMeta entityMeta = context.getEntityMeta();
		BoundStatementWrapper bsWrapper = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta,
				context.getPrimaryKey());

		return context.executeImmediateWithConsistency(bsWrapper, readLevel).all();
	}

	private Row returnFirstRowOrNull(List<Row> rows) {
		if (rows.isEmpty()) {
			return null;
		} else {
			return rows.get(0);
		}
	}

	public ResultSet execute(Query query, Object... boundValues) {
		logDMLStatement(query, boundValues);
		return session.execute(query);
	}

	public PreparedStatement prepare(Statement statement) {
		return session.prepare(statement.getQueryString());
	}

	public ResultSet bindAndExecute(PreparedStatement ps, Object... params) {
		BoundStatement bs = ps.bind(params);

		logDMLStatement(bs);
		return session.execute(bs);

	}

	public Session getSession() {
		return session;
	}

	private void logDMLStatement(Query query, Object... boundValues) {
		if (dmlLogger.isDebugEnabled()) {
			String queryType;
			String queryString;
			String consistencyLevel;
			if (BoundStatement.class.isInstance(query)) {
				PreparedStatement ps = BoundStatement.class.cast(query).preparedStatement();
				queryType = "Prepared statement";
				queryString = ps.getQueryString();
				consistencyLevel = query.getConsistencyLevel() == null ? "DEFAULT" : query.getConsistencyLevel().name();

			} else if (Statement.class.isInstance(query)) {
				Statement statement = Statement.class.cast(query);
				queryType = "Simple query";
				queryString = statement.getQueryString();
				consistencyLevel = statement.getConsistencyLevel() == null ? "DEFAULT" : statement
						.getConsistencyLevel().name();
			} else {
				queryType = "Unknown query";
				queryString = "???";
				consistencyLevel = "UNKNWON";
			}

			dmlLogger.debug("{} : [{}] with CONSISTENCY LEVEL [{}]", queryType, queryString, consistencyLevel);
			if (boundValues != null && boundValues.length > 0) {
				dmlLogger.debug("\t bound values: {}", Arrays.asList(boundValues));
			}

		}
	}

	private ConsistencyLevel getReadConsistencyLevel(PersistenceContext context, EntityMeta entityMeta) {
		ConsistencyLevel readLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
				: entityMeta.getReadConsistencyLevel();
		return readLevel;
	}

	private ConsistencyLevel getWriteConsistencyLevel(PersistenceContext context, EntityMeta entityMeta) {
		ConsistencyLevel writeLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
				: entityMeta.getWriteConsistencyLevel();
		return writeLevel;
	}

	private ConsistencyLevel getReadConsistencyLevel(PersistenceContext context, PropertyMeta pm) {
		ConsistencyLevel consistency = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
				: pm.getReadConsistencyLevel();
		return consistency;
	}

	private ConsistencyLevel getWriteConsistencyLevel(PersistenceContext context, PropertyMeta counterMeta) {
		ConsistencyLevel consistency = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
				: counterMeta.getWriteConsistencyLevel();
		return consistency;
	}

}
