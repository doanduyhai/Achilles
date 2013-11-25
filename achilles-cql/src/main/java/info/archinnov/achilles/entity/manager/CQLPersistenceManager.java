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
package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.CQLSliceQueryExecutor;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.interceptor.EntityLifeCycleListener;
import info.archinnov.achilles.query.cql.CQLNativeQueryBuilder;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryValidator;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import com.datastax.driver.core.Session;

public class CQLPersistenceManager extends PersistenceManager<CQLPersistenceContext> {
	private CQLSliceQueryExecutor sliceQueryExecutor;
	private CQLPersistenceContextFactory contextFactory;
	protected CQLDaoContext daoContext;

	private CQLTypedQueryValidator typedQueryValidator = new CQLTypedQueryValidator();

	protected CQLPersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, //
			CQLPersistenceContextFactory contextFactory, CQLDaoContext daoContext, ConfigurationContext configContext) {
		super(entityMetaMap, configContext);
		this.daoContext = daoContext;
		super.proxifier = new CQLEntityProxifier();
		super.entityValidator = new EntityValidator<CQLPersistenceContext>(proxifier);
		super.entityLifeCycleListener = new EntityLifeCycleListener<CQLPersistenceContext>(proxifier, entityMetaMap);
		this.contextFactory = contextFactory;
		this.sliceQueryExecutor = new CQLSliceQueryExecutor(contextFactory, configContext, daoContext);
	}

	@Override
	public <T> SliceQueryBuilder<CQLPersistenceContext, T> sliceQuery(Class<T> entityClass) {
		EntityMeta meta = entityMetaMap.get(entityClass);
		Validator.validateTrue(meta.isClusteredEntity(), "Cannot perform slice query on entity type '%s' because it is " + "not a clustered entity", meta.getClassName());
		return new SliceQueryBuilder<CQLPersistenceContext, T>(sliceQueryExecutor, entityClass, meta);
	}

	/**
	 * Return a CQL native query builder
	 * 
	 * @param queryString
	 *            native CQL query string, including limit, ttl and consistency
	 *            options
	 * 
	 * @return CQLNativeQueryBuilder
	 */
	public CQLNativeQueryBuilder nativeQuery(String queryString) {
		Validator.validateNotBlank(queryString, "The query string for native query should not be blank");
		return new CQLNativeQueryBuilder(daoContext, queryString);
	}

	/**
	 * Return a CQL typed query builder
	 * 
	 * All found entities will be in 'managed' state
	 * 
	 * @param entityClass
	 *            type of entity to be returned
	 * 
	 * @param queryString
	 *            native CQL query string, including limit, ttl and consistency
	 *            options
	 * 
	 * @return CQLTypedQueryBuilder<T>
	 */
	public <T> CQLTypedQueryBuilder<T> typedQuery(Class<T> entityClass, String queryString) {
		return typedQuery(entityClass, queryString, true);
	}

	private <T> CQLTypedQueryBuilder<T> typedQuery(Class<T> entityClass, String queryString, boolean normalizeQuery) {
		Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
		Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
		Validator.validateTrue(entityMetaMap.containsKey(entityClass), "Cannot perform typed query because the entityClass '%s' is not managed by Achilles", entityClass.getCanonicalName());

		EntityMeta meta = entityMetaMap.get(entityClass);
		typedQueryValidator.validateTypedQuery(entityClass, queryString, meta);
		return new CQLTypedQueryBuilder<T>(entityClass, daoContext, queryString, meta, contextFactory, true, normalizeQuery);
	}

	/**
	 * Return a CQL typed query builder
	 * 
	 * All found entities will be in 'managed' state
	 * 
	 * @param entityClass
	 *            type of entity to be returned
	 * 
	 * @param indexCondition
	 *            index condition
	 * 
	 * @return CQLTypedQueryBuilder<T>
	 */
	public <T> CQLTypedQueryBuilder<T> indexedQuery(Class<T> entityClass, IndexCondition indexCondition) {
		EntityMeta entityMeta = entityMetaMap.get(entityClass);

		Validator.validateFalse(entityMeta.isClusteredEntity(), "Index query is not supported for clustered entity");
		Validator.validateNotNull(indexCondition, "Index condition should not be null");
		Validator.validateNotBlank(indexCondition.getColumnName(), "Column name for index condition '%s' should be provided", indexCondition);
		Validator.validateNotNull(indexCondition.getColumnValue(), "Column value for index condition '%s' should be provided", indexCondition);
		Validator.validateNotNull(indexCondition.getIndexRelation(), "Index relation for index condition '%s' should be provided", indexCondition);

		StringBuilder queryBuilder = new StringBuilder("SELECT * FROM ");
		queryBuilder.append(entityMeta.getTableName()).append(" WHERE ");
		queryBuilder.append(indexCondition.generateWhereClause());
		return typedQuery(entityClass, queryBuilder.toString(), false);
	}

	/**
	 * Return a CQL typed query builder
	 * 
	 * All found entities will be returned as raw entities and not 'managed' by
	 * Achilles
	 * 
	 * @param entityClass
	 *            type of entity to be returned
	 * 
	 * @param queryString
	 *            native CQL query string, including limit, ttl and consistency
	 *            options
	 * 
	 * @return CQLTypedQueryBuilder<T>
	 */
	public <T> CQLTypedQueryBuilder<T> rawTypedQuery(Class<T> entityClass, String queryString) {
		Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
		Validator.validateNotBlank(queryString, "The query string for typed query should not be blank");
		Validator.validateTrue(entityMetaMap.containsKey(entityClass), "Cannot perform typed query because the entityClass '%s' is not managed by Achilles", entityClass.getCanonicalName());

		EntityMeta meta = entityMetaMap.get(entityClass);
		typedQueryValidator.validateRawTypedQuery(entityClass, queryString, meta);
		return new CQLTypedQueryBuilder<T>(entityClass, daoContext, queryString, meta, contextFactory, false, true);
	}

	@Override
	protected CQLPersistenceContext initPersistenceContext(Object entity, Options options) {
		return contextFactory.newContext(entity, options);
	}

	@Override
	protected CQLPersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
		return contextFactory.newContext(entityClass, primaryKey, options);
	}

	public Session getNativeSession() {
		return daoContext.getSession();
	}

}
