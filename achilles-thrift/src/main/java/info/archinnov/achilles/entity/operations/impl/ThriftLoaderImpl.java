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

import static info.archinnov.achilles.logger.ThriftLoggerHelper.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftLoaderImpl {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftLoaderImpl.class);

	private ThriftEntityMapper mapper = new ThriftEntityMapper();
	private ThriftCompositeFactory compositeFactory = new ThriftCompositeFactory();
	private ThriftCompositeTransformer compositeTransformer = new ThriftCompositeTransformer();

	public <T> T load(ThriftPersistenceContext context, Class<T> entityClass) {
		log.trace("Loading entity of class {} with primary key {}", context
				.getEntityClass().getCanonicalName(), context.getPrimaryKey());
		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();
		T entity = null;
		if (entityMeta.isClusteredEntity()) {
			entity = loadClusteredEntity(context, entityClass, entityMeta,
					primaryKey);
		} else {
			List<Pair<Composite, String>> columns = context.getEntityDao()
					.eagerFetchEntity(primaryKey);
			if (columns.size() > 0) {
				log.trace("Mapping data from Cassandra columns to entity");

				entity = entityMeta.<T> instanciate();
				mapper.setEagerPropertiesToEntity(primaryKey, columns,
						entityMeta, entity);
				entityMeta.getIdMeta().setValueToField(entity, primaryKey);
			}
		}
		return entity;
	}

	public Object loadSimpleProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		if (context.isClusteredEntity()) {
			Object embeddedId = context.getPrimaryKey();
			Object partitionKey = context.getPartitionKey();
			PropertyMeta idMeta = context.getIdMeta();
			Composite composite = compositeFactory.createBaseForClusteredGet(
					embeddedId, idMeta);
			if (log.isTraceEnabled()) {
				log.trace(
						"Loading simple property {} of clustered entity {} from column family {} with primary key {} and composite column name {}",
						propertyMeta.getPropertyName(), propertyMeta
								.getEntityClassName(), context.getEntityMeta()
								.getTableName(), context.getPrimaryKey(),
						format(composite));
			}
			Object value = context.getWideRowDao().getValue(partitionKey,
					composite);
			return propertyMeta.decode(value);
		} else {
			Composite composite = compositeFactory
					.createBaseForGet(propertyMeta);
			if (log.isTraceEnabled()) {
				log.trace(
						"Loading simple property {} of entity {} from column family {} with primary key {} and composite column name {}",
						propertyMeta.getPropertyName(), propertyMeta
								.getEntityClassName(), context.getEntityMeta()
								.getTableName(), context.getPrimaryKey(),
						format(composite));
			}
			return propertyMeta.forceDecodeFromJSON((String) context
					.getEntityDao()
					.getValue(context.getPrimaryKey(), composite));
		}
	}

	public List<Object> loadListProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		log.trace(
				"Loading list property {} of class {} from column family {} with primary key {}",
				propertyMeta.getPropertyName(), propertyMeta
						.getEntityClassName(), context.getEntityMeta()
						.getTableName(), context.getPrimaryKey());
		List<Pair<Composite, String>> columns = fetchColumns(context,
				propertyMeta);
		List<Object> list = null;
		if (columns.size() > 0) {
			list = new ArrayList<Object>();
			for (Pair<Composite, String> pair : columns) {
				int index = Integer.parseInt(pair.left.get(2, STRING_SRZ));
				list.add(index, propertyMeta.decode(pair.right));
			}
		}
		return list;
	}

	public Set<Object> loadSetProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		log.trace(
				"Loading set property {} of class {} from column family {} with primary key {}",
				propertyMeta.getPropertyName(), propertyMeta
						.getEntityClassName(), context.getEntityMeta()
						.getTableName(), context.getPrimaryKey());
		List<Pair<Composite, String>> columns = fetchColumns(context,
				propertyMeta);
		Set<Object> set = null;
		if (columns.size() > 0) {
			set = new HashSet<Object>();
			for (Pair<Composite, String> pair : columns) {
				set.add(propertyMeta.decode(pair.left.get(2, STRING_SRZ)));
			}
		}
		return set;
	}

	public Map<Object, Object> loadMapProperty(
			ThriftPersistenceContext context, PropertyMeta propertyMeta) {
		log.trace(
				"Loading map property {} of class {} from column family {} with primary key {}",
				propertyMeta.getPropertyName(), propertyMeta
						.getEntityClassName(), context.getEntityMeta()
						.getTableName(), context.getPrimaryKey());
		List<Pair<Composite, String>> columns = fetchColumns(context,
				propertyMeta);
		Map<Object, Object> map = null;
		if (columns.size() > 0) {
			map = new HashMap<Object, Object>();
			for (Pair<Composite, String> pair : columns) {
				Object key = propertyMeta.forceDecodeFromJSON(
						pair.left.get(2, STRING_SRZ),
						propertyMeta.getKeyClass());
				Object value = propertyMeta.decode(pair.right);
				map.put(key, value);
			}
		}
		return map;
	}

	protected List<Pair<Composite, String>> fetchColumns(
			ThriftPersistenceContext context, PropertyMeta propertyMeta) {

		Composite start = compositeFactory.createBaseForQuery(propertyMeta,
				EQUAL);
		Composite end = compositeFactory.createBaseForQuery(propertyMeta,
				GREATER_THAN_EQUAL);
		if (log.isTraceEnabled()) {
			log.trace(
					"Fetching columns from Cassandra with column names {} / {}",
					format(start), format(end));
		}
		List<Pair<Composite, String>> columns = context.getEntityDao()
				.findColumnsRange(context.getPrimaryKey(), start, end, false,
						Integer.MAX_VALUE);
		return columns;
	}

	private <T> T loadClusteredEntity(ThriftPersistenceContext context,
			Class<T> entityClass, EntityMeta entityMeta, Object primaryKey) {
		PropertyMeta idMeta = entityMeta.getIdMeta();
		Composite composite = compositeFactory.createBaseForClusteredGet(
				primaryKey, idMeta);
		Object partitionKey = entityMeta.getPartitionKey(primaryKey);

		T clusteredEntity;
		if (entityMeta.isValueless()) {
			HColumn<Composite, Object> column = context.getWideRowDao()
					.getColumn(partitionKey, composite);
			clusteredEntity = column != null ? compositeTransformer
					.buildClusteredEntityWithIdOnly(entityClass, context,
							column.getName().getComponents()) : null;
		} else if (entityMeta.isClusteredCounter()) {
			HCounterColumn<Composite> counterColumn = context.getWideRowDao()
					.getCounterColumn(partitionKey, composite);
			clusteredEntity = counterColumn != null ? compositeTransformer
					.buildClusteredEntityWithIdOnly(entityClass, context,
							counterColumn.getName().getComponents()) : null;
		} else {
			HColumn<Composite, Object> column = context.getWideRowDao()
					.getColumn(partitionKey, composite);
			clusteredEntity = column != null ? compositeTransformer
					.buildClusteredEntity(entityClass, context, column) : null;
		}
		return clusteredEntity;
	}
}
