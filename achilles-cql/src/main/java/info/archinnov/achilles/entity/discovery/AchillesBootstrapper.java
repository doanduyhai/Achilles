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

package info.archinnov.achilles.entity.discovery;

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.DaoContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.SchemaContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.helper.PropertyHelper;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class AchillesBootstrapper {
	private static final Logger log = LoggerFactory.getLogger(AchillesBootstrapper.class);

	private EntityParser entityParser = new EntityParser();
	private DaoContextFactory daoContextFactory = new DaoContextFactory();
    private PropertyHelper propertyHelper = new PropertyHelper();

	public List<Class<?>> discoverEntities(List<String> packageNames) {
		log.debug("Discovery of Achilles entity classes in packages {}", StringUtils.join(packageNames, ","));

		Set<Class<?>> candidateClasses = new HashSet<>();
		Reflections reflections = new Reflections(packageNames);
		candidateClasses.addAll(reflections.getTypesAnnotatedWith(Entity.class));
		return new ArrayList<>(candidateClasses);
	}

	public Pair<Map<Class<?>, EntityMeta>, Boolean> buildMetaDatas(ConfigurationContext configContext,
			List<Class<?>> entities) {
        log.debug("Build meta data for candidate entities");
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();
		boolean hasSimpleCounter = false;
		for (Class<?> entityClass : entities) {
			EntityParsingContext context = new EntityParsingContext(configContext, entityClass);
			EntityMeta entityMeta = entityParser.parseEntity(context);
			entityMetaMap.put(entityClass, entityMeta);
			hasSimpleCounter = context.hasSimpleCounter() || hasSimpleCounter;
		}
		return Pair.create(entityMetaMap, hasSimpleCounter);
	}

	public void validateOrCreateTables(SchemaContext schemaContext) {
        log.debug("Start schema validation/creation");
		Map<String, TableMetadata> tableMetaDatas = schemaContext.fetchTableMetaData();

		for (Entry<Class<?>, EntityMeta> entry : schemaContext.entityMetaEntrySet()) {
			EntityMeta entityMeta = entry.getValue();
			String tableName = entityMeta.getTableName().toLowerCase();

			if (tableMetaDatas.containsKey(tableName)) {
				schemaContext.validateForEntity(entityMeta, tableMetaDatas.get(tableName));
			} else {
				schemaContext.createTableForEntity(entry.getValue());
			}
		}

		if (schemaContext.hasSimpleCounter()) {
			if (tableMetaDatas.containsKey(CQL_COUNTER_TABLE)) {
				schemaContext.validateAchillesCounter();
			} else {
				schemaContext.createTableForCounter();
			}
		}
	}

	public DaoContext buildDaoContext(Session session, Map<Class<?>, EntityMeta> entityMetaMap,
			boolean hasSimpleCounter) {
        log.debug("Build DaoContext");
		return daoContextFactory.build(session, entityMetaMap, hasSimpleCounter);
	}

    public void addInterceptorsToEntityMetas(List<Interceptor<?>> interceptors, Map<Class<?>,
            EntityMeta> entityMetaMap) {
        for (Interceptor<?> interceptor : interceptors) {
            Class<?> entityClass = propertyHelper.inferEntityClassFromInterceptor(interceptor);
            EntityMeta entityMeta = entityMetaMap.get(entityClass);
            Validator.validateBeanMappingTrue(entityMeta != null, "The entity class '%s' is not found", entityClass.getCanonicalName());
            entityMeta.addInterceptor(interceptor);
        }
    }
}
