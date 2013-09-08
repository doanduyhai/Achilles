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

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.validation.Validator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

public class CQLPersistenceContext extends PersistenceContext {
	private CQLDaoContext daoContext;
	private CQLAbstractFlushContext<?> flushContext;
	private CQLEntityLoader loader = new CQLEntityLoader();
	private CQLEntityPersister persister = new CQLEntityPersister();
	private CQLEntityMerger merger = new CQLEntityMerger();
	private CQLEntityProxifier proxifier = new CQLEntityProxifier();
	private EntityRefresher<CQLPersistenceContext> refresher;

	public CQLPersistenceContext(EntityMeta entityMeta,
			ConfigurationContext configContext, CQLDaoContext daoContext,
			CQLAbstractFlushContext<?> flushContext, Class<?> entityClass,
			Object primaryKey, Options options, Set<String> entitiesIdentity) {
		super(entityMeta, configContext, entityClass, primaryKey, flushContext,
				options, entitiesIdentity);
		initCollaborators(daoContext, flushContext);
	}

	public CQLPersistenceContext(EntityMeta entityMeta,
			ConfigurationContext configContext, CQLDaoContext daoContext,
			CQLAbstractFlushContext<?> flushContext, Object entity,
			Options options, Set<String> entitiesIdentity) {
		super(entityMeta, configContext, entity, flushContext, options,
				entitiesIdentity);
		initCollaborators(daoContext, flushContext);
	}

	private void initCollaborators(CQLDaoContext daoContext,
			CQLAbstractFlushContext<?> flushContext) {
		this.refresher = new EntityRefresher<CQLPersistenceContext>(loader,
				proxifier);
		this.daoContext = daoContext;
		this.flushContext = flushContext;
	}

	@Override
	public CQLPersistenceContext createContextForJoin(EntityMeta joinMeta,
			Object joinEntity) {
		Validator.validateNotNull(joinEntity, "join entity should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext,
				flushContext.duplicate(), joinEntity,
				options.duplicateWithoutTtlAndTimestamp(), entitiesIdentity);
	}

	@Override
	public CQLPersistenceContext createContextForJoin(Class<?> entityClass,
			EntityMeta joinMeta, Object joinId) {
		Validator
				.validateNotNull(entityClass, "entityClass should not be null");
		Validator.validateNotNull(joinId, "joinId should not be null");
		return new CQLPersistenceContext(joinMeta, configContext, daoContext,
				flushContext.duplicate(), entityClass, joinId,
				options.duplicateWithoutTtlAndTimestamp(), entitiesIdentity);
	}

	@Override
	public CQLPersistenceContext duplicate(Object entity) {
		return new CQLPersistenceContext(entityMeta, configContext, daoContext,
				flushContext.duplicate(), entity,
				options.duplicateWithoutTtlAndTimestamp(),
				new HashSet<String>());
	}

	public boolean checkForEntityExistence() {
		return daoContext.checkForEntityExistence(this);
	}

	public Row eagerLoadEntity() {
		return daoContext.eagerLoadEntity(this);
	}

	public Row loadProperty(PropertyMeta pm) {
		return daoContext.loadProperty(this, pm);
	}

	public void pushInsertStatement() {
		daoContext.pushInsertStatement(this);
	}

	public void pushUpdateStatement(List<PropertyMeta> pms) {
		daoContext.pushUpdateStatement(this, pms);
	}

	public void bindForRemoval(String tableName) {
		daoContext.bindForRemoval(this, tableName);
	}

	// Simple counter
	public void bindForSimpleCounterIncrement(PropertyMeta counterMeta,
			Long increment) {
		daoContext.bindForSimpleCounterIncrement(this, entityMeta, counterMeta,
				increment);
	}

	public void incrementSimpleCounter(PropertyMeta counterMeta,
			Long increment, ConsistencyLevel consistency) {
		daoContext.incrementSimpleCounter(this, entityMeta, counterMeta,
				increment, consistency);
	}

	public void decrementSimpleCounter(PropertyMeta counterMeta,
			Long decrement, ConsistencyLevel consistency) {
		daoContext.decrementSimpleCounter(this, entityMeta, counterMeta,
				decrement, consistency);
	}

	public Long getSimpleCounter(PropertyMeta counterMeta,
			ConsistencyLevel consistency) {
		Row row = daoContext.getSimpleCounter(this, counterMeta, consistency);
		if (row != null) {
			return row.getLong(CQL_COUNTER_VALUE);
		}
		return null;
	}

	public void bindForSimpleCounterRemoval(PropertyMeta counterMeta) {
		daoContext.bindForSimpleCounterDelete(this, entityMeta, counterMeta,
				primaryKey);
	}

	// Clustered counter
	public void pushClusteredCounterIncrementStatement(
			PropertyMeta counterMeta, Long increment) {
		daoContext.pushClusteredCounterIncrementStatement(this, entityMeta,
				counterMeta, increment);
	}

	public void incrementClusteredCounter(PropertyMeta counterMeta,
			Long increment, ConsistencyLevel consistency) {
		daoContext.incrementClusteredCounter(this, entityMeta, counterMeta,
				increment, consistency);
	}

	public void decrementClusteredCounter(PropertyMeta counterMeta,
			Long decrement, ConsistencyLevel consistency) {
		daoContext.decrementClusteredCounter(this, entityMeta, counterMeta,
				decrement, consistency);
	}

	public Long getClusteredCounter(PropertyMeta counterMeta,
			ConsistencyLevel readLevel) {
		Row row = daoContext.getClusteredCounter(this, counterMeta, readLevel);
		if (row != null) {
			return row.getLong(counterMeta.getPropertyName());
		}
		return null;
	}

	public void bindForClusteredCounterRemoval(PropertyMeta counterMeta) {
		daoContext.bindForClusteredCounterDelete(this, entityMeta, counterMeta,
				primaryKey);
	}

	public ResultSet bindAndExecute(PreparedStatement ps, Object... params) {
		return daoContext.bindAndExecute(ps, params);
	}

	public void pushBoundStatement(BoundStatementWrapper bsWrapper,
			ConsistencyLevel writeLevel) {
		flushContext.pushBoundStatement(bsWrapper, writeLevel);
	}

	public void pushStatement(Statement statement, ConsistencyLevel writeLevel) {
		flushContext.pushStatement(statement, writeLevel);
	}

	public ResultSet executeImmediateWithConsistency(
			BoundStatementWrapper bsWrapper,
			ConsistencyLevel readConsistencyLevel) {
		return flushContext.executeImmediateWithConsistency(bsWrapper.getBs(),
				readConsistencyLevel, bsWrapper.getValues());
	}

	@Override
	public void persist() {
		persister.persist(this);
		flush();
	}

	@Override
	public <T> T merge(T entity) {
		T merged = merger.merge(this, entity);
		flush();
		return merged;
	}

	@Override
	public void remove() {
		persister.remove(this);
		flush();
	}

	@Override
	public <T> T find(Class<T> entityClass) {
		T entity = loader.<T> load(this, entityClass);

		if (entity != null) {
			entity = proxifier.buildProxy(entity, this);
		}
		return entity;
	}

	@Override
	public <T> T getReference(Class<T> entityClass) {
		setLoadEagerFields(false);
		return find(entityClass);
	}

	@Override
	public void refresh() throws AchillesStaleObjectStateException {
		refresher.refresh(this);
	}

	@Override
	public <T> T initialize(T entity) {
		final EntityInterceptor<CQLPersistenceContext, T> interceptor = proxifier
				.getInterceptor(entity);
		initializer.initializeEntity(entity, entityMeta, interceptor);
		return entity;
	}
}
