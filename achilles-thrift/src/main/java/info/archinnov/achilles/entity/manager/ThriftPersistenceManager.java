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

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.ThriftPersistenceContextFactory;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftSliceQueryExecutor;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftPersistenceManager extends PersistenceManager<ThriftPersistenceContext> {
	private static final Logger log = LoggerFactory.getLogger(ThriftPersistenceManager.class);

	protected ThriftDaoContext daoContext;
	protected ThriftPersistenceContextFactory contextFactory;
	private ThriftSliceQueryExecutor sliceQueryExecutor;


	ThriftPersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, //
			ThriftPersistenceContextFactory contextFactory, ThriftDaoContext daoContext, //
			ConfigurationContext configContext) {
		super(entityMetaMap, configContext);
		this.contextFactory = contextFactory;
		this.daoContext = daoContext;
		super.proxifier = new ThriftEntityProxifier();
		super.entityValidator = new EntityValidator<ThriftPersistenceContext>(super.proxifier);
		this.sliceQueryExecutor = new ThriftSliceQueryExecutor(contextFactory, configContext);
	}

	/**
	 * Create a new slice query builder for entity of type T<br/>
	 * <br/>
	 * 
	 * @param entityClass
	 *            Entity class
	 * @return SliceQueryBuilder<T>
	 */
	@Override
	public <T> SliceQueryBuilder<ThriftPersistenceContext, T> sliceQuery(Class<T> entityClass) {
		EntityMeta meta = entityMetaMap.get(entityClass);
        Validator.validateTrue(meta.isClusteredEntity(), "Cannot perform slice query on entity type '%s' " +
                        "because it is not a clustered entity", meta.getClassName());
		return new SliceQueryBuilder<ThriftPersistenceContext, T>(sliceQueryExecutor,
				entityClass, meta);
	}

	@Override
	protected ThriftPersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
		return contextFactory.newContext(entityClass, primaryKey, options);
	}

	@Override
	protected ThriftPersistenceContext initPersistenceContext(Object entity, Options options) {
		return contextFactory.newContext(entity, options);
	}

	protected void setThriftDaoContext(ThriftDaoContext thriftDaoContext) {
		this.daoContext = thriftDaoContext;
	}

	protected void setQueryExecutor(ThriftSliceQueryExecutor queryExecutor) {
		this.sliceQueryExecutor = queryExecutor;
	}

	protected void setContextFactory(ThriftPersistenceContextFactory contextFactory) {
		this.contextFactory = contextFactory;
	}

}
