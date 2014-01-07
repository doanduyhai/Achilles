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
package info.archinnov.achilles.query.typed;

import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.statement.wrapper.SimpleStatementWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

public class TypedQueryBuilder<T> {
	private static final Logger log = LoggerFactory.getLogger(TypedQueryBuilder.class);

	private DaoContext daoContext;
	private String normalizedQuery;
	private Map<String, PropertyMeta> propertiesMap;
	private EntityMeta meta;
	private PersistenceContextFactory contextFactory;
	private boolean managed;
	private Object[] boundValues;

	private EntityMapper mapper = new EntityMapper();
	private EntityProxifier proxifier = new EntityProxifier();

	public TypedQueryBuilder(Class<T> entityClass, DaoContext daoContext, String queryString, EntityMeta meta,
			PersistenceContextFactory contextFactory, boolean managed, boolean shouldNormalizeQuery,
			Object[] boundValues) {
		this.daoContext = daoContext;
		this.boundValues = boundValues;
		this.normalizedQuery = shouldNormalizeQuery ? queryString.toLowerCase() : queryString;
		this.meta = meta;
		this.contextFactory = contextFactory;
		this.managed = managed;
		this.propertiesMap = transformPropertiesMap(meta);
	}

	/**
	 * Executes the query and returns entities
	 * 
	 * Matching CQL rows are mapped to entities by reflection. All un-mapped
	 * columns are ignored.
	 * 
	 * The size of the list is equal or lesser than the number of matching CQL
	 * row, because some null or empty rows are ignored and filtered out
	 * 
	 * @return List<T> list of found entities or empty list
	 * 
	 */
	public List<T> get() {
		log.debug("Get results for typed query {}", normalizedQuery);
		List<T> result = new ArrayList();
		List<Row> rows = daoContext.execute(new SimpleStatementWrapper(normalizedQuery, boundValues)).all();
		for (Row row : rows) {
			T entity = mapper.mapRowToEntityWithPrimaryKey(meta, row, propertiesMap, managed);
			if (entity != null) {
                meta.intercept(entity, Event.POST_LOAD);
				if (managed) {
					entity = buildProxy(entity);
				}
				result.add(entity);
			}
		}
		return result;
	}

	/**
	 * Executes the query and returns first entity
	 * 
	 * Matching CQL row is mapped to entity by reflection. All un-mapped columns
	 * are ignored.
	 * 
	 * @return T first found entity or null
	 * 
	 */
	public T getFirst() {
		log.debug("Get first result for typed query {}", normalizedQuery);
		T entity = null;
		Row row = daoContext.execute(new SimpleStatementWrapper(normalizedQuery, boundValues)).one();
		if (row != null) {
			entity = mapper.mapRowToEntityWithPrimaryKey(meta, row, propertiesMap, managed);
            meta.intercept(entity, Event.POST_LOAD);
			if (entity != null && managed) {
				entity = buildProxy(entity);
			}
		}
		return entity;
	}

	private Map<String, PropertyMeta> transformPropertiesMap(EntityMeta meta) {
		Map<String, PropertyMeta> propertiesMap = new HashMap();
		for (Entry<String, PropertyMeta> entry : meta.getPropertyMetas().entrySet()) {
			String propertyName = entry.getKey().toLowerCase();
			propertiesMap.put(propertyName, entry.getValue());
		}
		return propertiesMap;
	}

	private T buildProxy(T entity) {
		PersistenceContext context = contextFactory.newContext(entity);
		entity = proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context);
		return entity;
	}
}
