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
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;

public class CQLPersistenceContextFactory implements PersistenceContextFactory {

	public static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

	private CQLDaoContext daoContext;
	private ConfigurationContext configContext;
	private Map<Class<?>, EntityMeta> entityMetaMap;
	private CQLEntityProxifier proxifier = new CQLEntityProxifier();
	private ReflectionInvoker invoker = new ReflectionInvoker();

	public CQLPersistenceContextFactory(CQLDaoContext daoContext, ConfigurationContext configContext,
			Map<Class<?>, EntityMeta> entityMetaMap) {
		this.daoContext = daoContext;
		this.configContext = configContext;
		this.entityMetaMap = entityMetaMap;
	}

	@Override
	public CQLPersistenceContext newContext(Object entity, Options options) {
		Validator.validateNotNull(entity, "entity should not be null for persistence context creation");
		Class<?> entityClass = proxifier.deriveBaseClass(entity);
		EntityMeta meta = entityMetaMap.get(entityClass);
		CQLImmediateFlushContext flushContext = buildImmediateFlushContext(options);

		return new CQLPersistenceContext(meta, configContext, daoContext, flushContext, entity, options);
	}

	@Override
	public CQLPersistenceContext newContext(Object entity) {
		return newContext(entity, OptionsBuilder.noOptions());
	}

	@Override
	public CQLPersistenceContext newContext(Class<?> entityClass, Object primaryKey, Options options) {
		Validator.validateNotNull(entityClass, "entityClass should not be null for persistence context creation");
		Validator.validateNotNull(primaryKey, "primaryKey should not be null for persistence context creation");
		EntityMeta meta = entityMetaMap.get(entityClass);
		CQLImmediateFlushContext flushContext = buildImmediateFlushContext(options);

		return new CQLPersistenceContext(meta, configContext, daoContext, flushContext, entityClass, primaryKey,
				options);
	}

	@Override
	public CQLPersistenceContext newContextForSliceQuery(Class<?> entityClass, List<Object> partitionComponents,
			ConsistencyLevel cl) {
		EntityMeta meta = entityMetaMap.get(entityClass);
		PropertyMeta idMeta = meta.getIdMeta();
		Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionComponents(idMeta, partitionComponents);

		CQLImmediateFlushContext flushContext = buildImmediateFlushContext(OptionsBuilder.withConsistency(cl));

		return new CQLPersistenceContext(meta, configContext, daoContext, flushContext, entityClass, embeddedId,
				OptionsBuilder.withConsistency(cl));
	}

	private CQLImmediateFlushContext buildImmediateFlushContext(Options options) {
		return new CQLImmediateFlushContext(daoContext, options.getConsistencyLevel().orNull());
	}
}
