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

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftPersisterImpl;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftEntityPersister implements
		EntityPersister<ThriftPersistenceContext> {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftEntityPersister.class);

	private ThriftEntityLoader loader = new ThriftEntityLoader();
	private ThriftPersisterImpl persisterImpl = new ThriftPersisterImpl();

	@Override
	public void persist(ThriftPersistenceContext context) {
		EntityMeta entityMeta = context.getEntityMeta();
		Object entity = context.getEntity();

		if (context.addToProcessingList(entity)) {
			log.debug("Persisting transient entity {}", entity);
			if (context.isClusteredEntity()) {
				persistClusteredEntity(context);
			} else {
				// Remove first
				persisterImpl.removeEntityBatch(context);

				for (PropertyMeta propertyMeta : entityMeta.getPropertyMetas()
						.values()) {
					this.persistPropertyBatch(context, propertyMeta);
				}
			}
		}
	}

	public void persistPropertyBatch(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		log.debug("Persisting property {} of entity {}",
				propertyMeta.getPropertyName(), context.getEntity());
		switch (propertyMeta.type()) {
		case ID:
		case SIMPLE:
		case LAZY_SIMPLE:
			persisterImpl.batchPersistSimpleProperty(context, propertyMeta);
			break;
		case LIST:
		case LAZY_LIST:
			batchPersistListProperty(context, propertyMeta);
			break;
		case SET:
		case LAZY_SET:
			batchPersistSetProperty(context, propertyMeta);
			break;
		case MAP:
		case LAZY_MAP:
			batchPersistMapProperty(context, propertyMeta);
			break;
		case JOIN_SIMPLE:
			batchPersistJoinEntity(context, propertyMeta);
			break;
		case JOIN_LIST:
			batchPersistJoinListProperty(context, propertyMeta);
			break;
		case JOIN_SET:
			batchPersistJoinSetProperty(context, propertyMeta);
			break;
		case JOIN_MAP:
			batchPersistJoinMapProperty(context, propertyMeta);
			break;
		case COUNTER:
			persisterImpl.persistCounter(context, propertyMeta);
			break;
		default:
			break;
		}
	}

	@Override
	public void remove(ThriftPersistenceContext context) {
		log.debug("Removing entity of class {} and primary key {} ", context
				.getEntityClass().getCanonicalName(), context.getPrimaryKey());
		EntityMeta meta = context.getEntityMeta();

		if (meta.isClusteredEntity()) {
			removeClusteredEntity(context);
		} else {
			persisterImpl.remove(context);
		}
	}

	public void removePropertyBatch(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		log.debug(
				"Removing property {} from entity of class {} and primary key {} ",
				propertyMeta.getPropertyName(), context.getEntityClass()
						.getCanonicalName(), context.getPrimaryKey());

		persisterImpl.removePropertyBatch(context, propertyMeta);
	}

	public Object cascadePersistOrEnsureExists(
			ThriftPersistenceContext context, Object joinEntity,
			JoinProperties joinProperties) {

		EntityMeta joinMeta = joinProperties.getEntityMeta();
		Object joinId = joinMeta.getPrimaryKey(joinEntity);
		Validate.notNull(joinId,
				"key value for entity '" + joinMeta.getClassName()
						+ "' should not be null");

		Set<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(ALL) || cascadeTypes.contains(PERSIST)) {
			log.debug(
					"Cascade-persisting entity of class {} and primary key {} ",
					context.getEntityClass().getCanonicalName(),
					context.getPrimaryKey());

			persist(context);
		} else if (context.isEnsureJoinConsistency()
				&& context.addToProcessingList(joinEntity)) {

			log.debug(
					"Consistency check for join entity of class {} and primary key {} ",
					context.getEntityClass().getCanonicalName(),
					context.getPrimaryKey());

			Object primaryKey = loader.loadPrimaryKey(context,
					joinMeta.getIdMeta());

			Validator
					.validateNotNull(
							primaryKey,
							"The entity '"
									+ joinProperties.getEntityMeta()
											.getClassName()
									+ "' with id '"
									+ joinId
									+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
		}

		return joinId;

	}

	public void persistClusteredValue(ThriftPersistenceContext context,
			Object clusteredValue) {
		Object primaryKey = context.getPrimaryKey();
		Object partitionKey = context.getEntityMeta().getPartitionKey(
				primaryKey);
		persisterImpl.persistClusteredValueBatch(context, partitionKey,
				clusteredValue, this);
	}

	private void batchPersistListProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		List<?> list = propertyMeta.getListValueFromField(context.getEntity());
		if (list != null) {
			persisterImpl.batchPersistList(list, context, propertyMeta);
		}
	}

	private void batchPersistSetProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		Set<?> set = propertyMeta.getSetValueFromField(context.getEntity());
		if (set != null) {
			persisterImpl.batchPersistSet(set, context, propertyMeta);
		}
	}

	private void batchPersistMapProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		Map<?, ?> map = propertyMeta.getMapValueFromField(context.getEntity());
		if (map != null) {
			persisterImpl.batchPersistMap(map, context, propertyMeta);
		}
	}

	private void batchPersistJoinEntity(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		Object joinEntity = propertyMeta.getValueFromField(context.getEntity());

		if (joinEntity != null) {
			persisterImpl.batchPersistJoinEntity(context, propertyMeta,
					joinEntity, this);
		}
	}

	private void batchPersistJoinListProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {

		List<?> joinList = propertyMeta.getListValueFromField(context
				.getEntity());
		if (joinList != null) {
			persisterImpl.batchPersistJoinList(context, propertyMeta, joinList,
					this);
		}
	}

	private void batchPersistJoinSetProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {

		Set<?> joinSet = propertyMeta.getSetValueFromField(context.getEntity());
		if (joinSet != null) {
			persisterImpl.batchPersistJoinSet(context, propertyMeta, joinSet,
					this);
		}
	}

	private void batchPersistJoinMapProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		Map<?, ?> joinMap = propertyMeta.getMapValueFromField(context
				.getEntity());

		if (joinMap != null) {
			persisterImpl.batchPersistJoinMap(context, propertyMeta, joinMap,
					this);
		}

	}

	private void persistClusteredEntity(ThriftPersistenceContext context) {
		Object entity = context.getEntity();
		Object compoundKey = context.getPrimaryKey();
		String className = context.getEntityClass().getCanonicalName();

		Validator
				.validateNotNull(
						compoundKey,
						"Compound key should be provided for clustered entity '%s' persistence",
						className);
		Validator
				.validateNotNull(
						entity,
						"Entity should be provided for clustered entity '%s' persistence",
						className);

		Object partitionKey = context.getEntityMeta().getPartitionKey(
				compoundKey);

		Object clusteredValue;
		if (context.isValueless()) {
			clusteredValue = "";
		} else {
			PropertyMeta pm = context.getFirstMeta();
			clusteredValue = pm.getValueFromField(entity);
			Validator
					.validateNotNull(
							clusteredValue,
							"Property '%s' should not be null for clustered entity '%s' persistence",
							pm.getPropertyName(), className);
		}

		persisterImpl.persistClusteredEntity(this, context, partitionKey,
				clusteredValue);
	}

	private void removeClusteredEntity(ThriftPersistenceContext context) {
		Object embeddedId = context.getPrimaryKey();
		String className = context.getEntityClass().getCanonicalName();

		Validator
				.validateNotNull(
						embeddedId,
						"Embedded id should be provided for clustered entity '%s' persistence",
						className);

		Object partitionKey = context.getEntityMeta().getPartitionKey(
				embeddedId);

		persisterImpl.removeClusteredEntity(context, partitionKey);
	}
}
