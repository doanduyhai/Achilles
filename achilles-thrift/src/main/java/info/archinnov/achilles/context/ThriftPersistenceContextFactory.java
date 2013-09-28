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

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftPersistenceContextFactory implements
		PersistenceContextFactory {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftPersistenceContextFactory.class);

	private ThriftDaoContext daoContext;
	private ConfigurationContext configContext;
	private Map<Class<?>, EntityMeta> entityMetaMap;

	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
	private ReflectionInvoker invoker = new ReflectionInvoker();

	public ThriftPersistenceContextFactory(ThriftDaoContext daoContext,
			ConfigurationContext configContext,
			Map<Class<?>, EntityMeta> entityMetaMap) {
		this.daoContext = daoContext;
		this.configContext = configContext;
		this.entityMetaMap = entityMetaMap;
	}

	@Override
	public ThriftPersistenceContext newContext(Object entity, Options options) {
		log.trace("Initializing new persistence context for entity {}", entity);
		Validator.validateNotNull(entity,
				"entity should not be null for persistence context creation");
		Class<?> entityClass = proxifier.deriveBaseClass(entity);
		EntityMeta meta = entityMetaMap.get(entityClass);
		ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(options);

		return new ThriftPersistenceContext(meta, configContext, daoContext,
				flushContext, entity, options);
	}

	@Override
	public ThriftPersistenceContext newContext(Object entity) {
		return newContext(entity, OptionsBuilder.noOptions());
	}

	@Override
	public ThriftPersistenceContext newContext(Class<?> entityClass,
			Object primaryKey, Options options) {
		Validator
				.validateNotNull(entityClass,
						"entityClass should not be null for persistence context creation");
		Validator
				.validateNotNull(primaryKey,
						"primaryKey should not be null for persistence context creation");
		EntityMeta meta = entityMetaMap.get(entityClass);
		ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(options);

		return new ThriftPersistenceContext(meta, configContext, daoContext,
				flushContext, entityClass, primaryKey, options);
	}

	@Override
	public ThriftPersistenceContext newContextForSliceQuery(
			Class<?> entityClass, Object partitionKey, ConsistencyLevel cl) {
		EntityMeta meta = entityMetaMap.get(entityClass);
		PropertyMeta idMeta = meta.getIdMeta();
		Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionKey(
				idMeta, partitionKey);

		ThriftImmediateFlushContext flushContext = buildImmediateFlushContext(OptionsBuilder
				.withConsistency(cl));

		return new ThriftPersistenceContext(meta, configContext, daoContext,
				flushContext, entityClass, embeddedId,
				OptionsBuilder.withConsistency(cl));
	}

	private ThriftImmediateFlushContext buildImmediateFlushContext(
			Options options) {
		return new ThriftImmediateFlushContext(daoContext,
				configContext.getConsistencyPolicy(), options
						.getConsistencyLevel().orNull());
	}

}
