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

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.datastax.driver.core.Row;

public class CQLLoaderImpl {
	private ReflectionInvoker invoker = new ReflectionInvoker();
	private CQLEntityMapper mapper = new CQLEntityMapper();
	private CQLRowMethodInvoker cqlRowInvoker = new CQLRowMethodInvoker();

	public <T> T eagerLoadEntity(CQLPersistenceContext context,
			Class<T> entityClass) {
		EntityMeta entityMeta = context.getEntityMeta();

		T entity = null;

		if (entityMeta.isClusteredCounter()) {
			PropertyMeta counterMeta = entityMeta.getFirstMeta();
			ConsistencyLevel readLevel = context.getConsistencyLevel()
					.isPresent() ? context.getConsistencyLevel().get()
					: counterMeta.getReadConsistencyLevel();
			Long counterValue = context.getClusteredCounter(counterMeta,
					readLevel);
			if (counterValue != null) {
				entity = invoker.instanciate(entityClass);
			}
		} else {
			Row row = context.eagerLoadEntity();
			if (row != null) {
				entity = invoker.instanciate(entityClass);
				mapper.setEagerPropertiesToEntity(row, entityMeta, entity);
			}
		}
		return entity;
	}

	public void loadPropertyIntoEntity(CQLPersistenceContext context,
			PropertyMeta pm, Object entity) {
		Row row = context.loadProperty(pm);
		mapper.setPropertyToEntity(row, pm, entity);
	}

	public void loadJoinPropertyIntoEntity(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm, Object entity) {
		Row row = context.loadProperty(pm);

		if (row != null) {
			EntityMeta joinMeta = pm.getJoinProperties().getEntityMeta();
			PropertyMeta joinIdMeta = joinMeta.getIdMeta();

			Object joinValue = null;
			switch (pm.type()) {
			case JOIN_SIMPLE:
				joinValue = loadJoinSimple(loader, context, pm, row, joinMeta,
						joinIdMeta, joinValue);
				break;
			case JOIN_LIST:
				joinValue = loadJoinList(loader, context, pm, row, joinMeta,
						joinIdMeta, joinValue);
				break;
			case JOIN_SET:
				joinValue = loadJoinSet(loader, context, pm, row, joinMeta,
						joinIdMeta, joinValue);
				break;
			case JOIN_MAP:
				joinValue = loadJoinMap(loader, context, pm, row, joinMeta,
						joinValue);
				break;
			default:
				break;
			}

			mapper.setJoinValueToEntity(joinValue, pm, entity);
		}
	}

	private Object loadJoinList(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm, Row row,
			EntityMeta joinMeta, PropertyMeta joinIdMeta, Object joinValue) {
		List<Object> joinEntitiesList = new ArrayList<Object>();
		List<?> joinIdsList = cqlRowInvoker.invokeOnRowForList(row, pm,
				pm.getPropertyName(), joinIdMeta.getValueClass());
		if (joinIdsList != null && !joinIdsList.isEmpty()) {
			joinValue = loadJoinEntitiesForCollection(loader, context, pm,
					joinMeta, joinIdsList, joinEntitiesList);
		}
		return joinValue;
	}

	private Object loadJoinSet(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm, Row row,
			EntityMeta joinMeta, PropertyMeta joinIdMeta, Object joinValue) {
		Set<Object> joinEntitiesSet = new HashSet<Object>();
		Set<?> joinIdsSet = cqlRowInvoker.invokeOnRowForSet(row, pm,
				pm.getPropertyName(), joinIdMeta.getValueClass());
		if (joinIdsSet != null && !joinIdsSet.isEmpty()) {
			joinValue = loadJoinEntitiesForCollection(loader, context, pm,
					joinMeta, joinIdsSet, joinEntitiesSet);
		}
		return joinValue;
	}

	private Object loadJoinMap(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm, Row row,
			EntityMeta joinMeta, Object joinValue) {
		Map<Object, Object> joinEntitiesMap = new HashMap<Object, Object>();
		Class<?> keyClass = pm.getKeyClass();
		Class<?> valueClass = pm.getJoinProperties().getEntityMeta()
				.getIdMeta().getValueClass();
		Map<?, ?> joinIdsMap = cqlRowInvoker.invokeOnRowForMap(row, pm,
				pm.getPropertyName(), keyClass, valueClass);
		if (joinIdsMap != null && !joinIdsMap.isEmpty()) {
			joinValue = loadJoinEntitiesForMap(loader, context, pm, joinMeta,
					joinIdsMap, joinEntitiesMap);
		}
		return joinValue;
	}

	private Object loadJoinSimple(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm, Row row,
			EntityMeta joinMeta, PropertyMeta joinIdMeta, Object joinValue) {
		Object joinId = cqlRowInvoker.invokeOnRowForProperty(row, pm,
				pm.getPropertyName(), joinIdMeta.getValueClass());
		if (joinId != null) {
			joinValue = loadJoinEntity(loader, context, pm, joinMeta, joinId);
		}
		return joinValue;
	}

	private Object loadJoinEntity(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm,
			EntityMeta joinMeta, Object joinId) {
		CQLPersistenceContext joinContext = context.createContextForJoin(
				pm.getValueClass(), joinMeta, joinId);
		return loader.load(joinContext, pm.getValueClass());
	}

	private Object loadJoinEntitiesForCollection(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm,
			EntityMeta joinMeta, Collection<?> joinIds,
			Collection<Object> joinEntities) {
		for (Object joinId : joinIds) {
			joinEntities.add(loadJoinEntity(loader, context, pm, joinMeta,
					joinId));
		}

		return joinEntities;
	}

	private Object loadJoinEntitiesForMap(CQLEntityLoader loader,
			CQLPersistenceContext context, PropertyMeta pm,
			EntityMeta joinMeta, Map<?, ?> joinIdMap,
			Map<Object, Object> joinEntitiesMap) {
		for (Entry<?, ?> entry : joinIdMap.entrySet()) {
			Object joinEntity = loadJoinEntity(loader, context, pm, joinMeta,
					entry.getValue());
			joinEntitiesMap.put(entry.getKey(), joinEntity);
		}

		return joinEntitiesMap;
	}
}
