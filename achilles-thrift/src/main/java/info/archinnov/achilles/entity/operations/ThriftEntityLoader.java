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

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftJoinLoaderImpl;
import info.archinnov.achilles.entity.operations.impl.ThriftLoaderImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftEntityLoader implements
		EntityLoader<ThriftPersistenceContext> {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftEntityLoader.class);

	private ThriftJoinLoaderImpl joinLoaderImpl = new ThriftJoinLoaderImpl();
	private ThriftLoaderImpl loaderImpl = new ThriftLoaderImpl();

	@Override
	public <T> T load(ThriftPersistenceContext context, Class<T> entityClass) {
		log.debug("Loading entity of class {} with primary key {}", context
				.getEntityClass().getCanonicalName(), context.getPrimaryKey());

		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass,
				"Entity class should not be null");
		Validator.validateNotNull(primaryKey,
				"Entity '%s' key should not be null",
				entityClass.getCanonicalName());
		Validator.validateNotNull(entityMeta,
				"Entity meta for '%s' should not be null",
				entityClass.getCanonicalName());

		T entity = null;
		try {
			if (context.isLoadEagerFields()) {
				entity = loaderImpl.load(context, entityClass);
			} else {
				log.debug("Get reference called, just instanciate the entity with primary key");
				entity = entityMeta.<T> instanciate();
				entityMeta.getIdMeta().setValueToField(entity, primaryKey);
			}
		} catch (Exception e) {
			throw new AchillesException("Error when loading entity type '"
					+ entityClass.getCanonicalName() + "' with key '"
					+ primaryKey + "'. Cause : " + e.getMessage(), e);
		}
		return entity;
	}

	@Override
	public <V> void loadPropertyIntoObject(ThriftPersistenceContext context,
			Object realObject, PropertyMeta propertyMeta) {
		log.debug(
				"Loading eager properties into entity of class {} with primary key {}",
				context.getEntityClass().getCanonicalName(),
				context.getPrimaryKey());

		Object value = null;
		switch (propertyMeta.type()) {
		case SIMPLE:
		case LAZY_SIMPLE:
			value = loaderImpl.loadSimpleProperty(context, propertyMeta);
			break;
		case LIST:
		case LAZY_LIST:
			value = loaderImpl.loadListProperty(context, propertyMeta);
			break;
		case SET:
		case LAZY_SET:
			value = loaderImpl.loadSetProperty(context, propertyMeta);
			break;
		case MAP:
		case LAZY_MAP:
			value = loaderImpl.loadMapProperty(context, propertyMeta);
			break;
		case JOIN_SIMPLE:
			value = loaderImpl.loadJoinSimple(context, propertyMeta, this);
			break;
		case JOIN_LIST:
			value = joinLoaderImpl.loadJoinListProperty(context, propertyMeta);
			break;
		case JOIN_SET:
			value = joinLoaderImpl.loadJoinSetProperty(context, propertyMeta);
			break;
		case JOIN_MAP:
			value = joinLoaderImpl.loadJoinMapProperty(context, propertyMeta);
			break;
		default:
			return;
		}
		propertyMeta.setValueToField(realObject, value);
	}

	protected Object loadPrimaryKey(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		return loaderImpl.loadSimpleProperty(context, propertyMeta);
	}
}
