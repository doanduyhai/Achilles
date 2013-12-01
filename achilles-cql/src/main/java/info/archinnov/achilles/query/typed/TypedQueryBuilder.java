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

import static info.archinnov.achilles.consistency.ConsistencyConvertor.getCQLLevel;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.statement.wrapper.SimpleStatementWrapper;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;

public class TypedQueryBuilder<T> {
	private static final Logger log = LoggerFactory.getLogger(TypedQueryBuilder.class);

	private static final Pattern SELECT_COLUMNS_EXTRACTION_PATTERN = Pattern.compile("^\\s*select\\s+(.+)\\s+from.+$");
	private static final String SELECT_STAR = "select * ";
	private static final String WHITE_SPACES = "\\s+";

	private DaoContext daoContext;
	private String normalizedQuery;
	private Map<String, PropertyMeta> propertiesMap;
	private EntityMeta meta;
	private PersistenceContextFactory contextFactory;
	private boolean managed;
	private List<String> selectedColumns;
	private Set<Method> alreadyLoaded;
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
		determineAlreadyLoadedSet();
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
		List<T> result = new ArrayList<T>();
		List<Row> rows = daoContext.execute(new SimpleStatementWrapper(normalizedQuery, boundValues)).all();
		for (Row row : rows) {
			T entity = mapper.mapRowToEntityWithPrimaryKey(meta, row, propertiesMap, managed);
			if (entity != null) {
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
		final ConsistencyLevel consistencyLevel = getCQLLevel(meta.getReadConsistencyLevel());
		Row row = daoContext.execute(new SimpleStatementWrapper(normalizedQuery, boundValues)).one();
		if (row != null) {
			entity = mapper.mapRowToEntityWithPrimaryKey(meta, row, propertiesMap, managed);
			if (entity != null && managed) {
				entity = buildProxy(entity);
			}
		}
		return entity;
	}

	private Map<String, PropertyMeta> transformPropertiesMap(EntityMeta meta) {
		Map<String, PropertyMeta> propertiesMap = new HashMap<String, PropertyMeta>();
		for (Entry<String, PropertyMeta> entry : meta.getPropertyMetas().entrySet()) {
			String propertyName = entry.getKey().toLowerCase();
			propertiesMap.put(propertyName, entry.getValue());
		}
		return propertiesMap;
	}

	private void determineAlreadyLoadedSet() {
		if (normalizedQuery.contains(SELECT_STAR)) {
			alreadyLoaded = new HashSet<Method>(meta.getEagerGetters());
		} else {
			alreadyLoaded = new HashSet<Method>();
			Matcher matcher = SELECT_COLUMNS_EXTRACTION_PATTERN.matcher(normalizedQuery);
			if (matcher.matches()) {
				selectedColumns = Arrays.asList(matcher.group(1).replaceAll(WHITE_SPACES, "").split(","));
				for (PropertyMeta pm : propertiesMap.values()) {
					if (selectedColumns.contains(pm.getPropertyName().toLowerCase())) {
						alreadyLoaded.add(pm.getGetter());
					}
				}
			}
		}
	}

	private T buildProxy(T entity) {
		PersistenceContext context = contextFactory.newContext(entity);
		entity = proxifier.buildProxy(entity, context, alreadyLoaded);
		return entity;
	}
}
