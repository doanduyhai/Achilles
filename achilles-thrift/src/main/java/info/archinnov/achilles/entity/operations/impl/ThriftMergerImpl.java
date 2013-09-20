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
package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.multiValuesNonProxyTypes;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftMergerImpl implements Merger<ThriftPersistenceContext> {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftMergerImpl.class);
	private ThriftEntityPersister persister = new ThriftEntityPersister();

	@Override
	public void merge(ThriftPersistenceContext context,
			Map<Method, PropertyMeta> dirtyMap) {
		if (dirtyMap.size() > 0) {
			Object entity = context.getEntity();
			EntityMeta meta = context.getEntityMeta();

			if (meta.isClusteredEntity()) {
				mergeClusteredEntity(context, dirtyMap, entity);
			} else {
				mergeEntity(context, dirtyMap, entity);
			}
		}

		dirtyMap.clear();

	}

	@Override
	public void cascadeMerge(
			EntityMerger<ThriftPersistenceContext> entityMerger,
			ThriftPersistenceContext context, List<PropertyMeta> joinPMs) {

		Object entity = context.getEntity();
		for (PropertyMeta pm : joinPMs) {
			log.debug("Cascade-merging join property {}", pm.getPropertyName());
			Object joinValue = pm.getValueFromField(entity);
			if (joinValue != null) {
				if (pm.isJoinCollection()) {
					doCascadeCollection(entityMerger, context, pm,
							(Collection<?>) joinValue);
				} else if (pm.isJoinMap()) {
					Map<?, ?> joinMap = (Map<?, ?>) joinValue;
					doCascadeCollection(entityMerger, context, pm,
							joinMap.values());
				} else {
					doCascade(entityMerger, context, pm, joinValue);
				}
			}
		}
	}

	private void doCascade(EntityMerger<ThriftPersistenceContext> entityMerger,
			ThriftPersistenceContext context, PropertyMeta pm, Object joinEntity) {

		if (joinEntity != null) {
			log.debug("Merging join entity {} ", joinEntity);
			ThriftPersistenceContext joinContext = context
					.createContextForJoin(pm.joinMeta(), joinEntity);

			entityMerger.merge(joinContext, joinEntity);
		}
	}

	private void doCascadeCollection(
			EntityMerger<ThriftPersistenceContext> entityMerger,
			ThriftPersistenceContext context, PropertyMeta pm,
			Collection<?> joinEntities) {
		log.debug("Merging join collection of entity {} ", joinEntities);
		for (Object joinEntity : joinEntities) {
			doCascade(entityMerger, context, pm, joinEntity);
		}
	}

	private void mergeEntity(ThriftPersistenceContext context,
			Map<Method, PropertyMeta> dirtyMap, Object entity) {
		for (Entry<Method, PropertyMeta> entry : dirtyMap.entrySet()) {
			PropertyMeta pm = entry.getValue();
			boolean removeProperty = pm.getValueFromField(entity) == null;

			if (removeProperty) {
				log.debug("Removing property {}", pm.getPropertyName());
				persister.removePropertyBatch(context, pm);
			} else {
				if (multiValuesNonProxyTypes.contains(pm.type())) {
					log.debug(
							"Removing dirty collection/map {} before merging",
							pm.getPropertyName());
					persister.removePropertyBatch(context, pm);
				}
				persister.persistPropertyBatch(context, pm);
			}
		}
	}

	private void mergeClusteredEntity(ThriftPersistenceContext context,
			Map<Method, PropertyMeta> dirtyMap, Object entity) {
		PropertyMeta pm = dirtyMap.entrySet().iterator().next().getValue();
		Object clusteredValue = pm.getValueFromField(entity);
		if (clusteredValue == null) {
			persister.remove(context);
		} else {
			persister.persistClusteredValue(context, clusteredValue);
		}
	}
}
