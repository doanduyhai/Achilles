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
package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityExplorer;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.EventInterceptor;
import info.archinnov.achilles.validation.Validator;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PersistenceManagerFactory {
	private static final Logger log = LoggerFactory.getLogger(PersistenceManagerFactory.class);

	protected Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
	protected ConfigurationContext configContext;
	protected List<String> entityPackages;
	protected List<EventInterceptor<?>> eventInterceptors;

	private EntityParser entityParser = new EntityParser();
	private EntityExplorer entityExplorer = new EntityExplorer();

	protected PersistenceManagerFactory(Map<String, Object> configurationMap, ArgumentExtractor argumentExtractor) {
		Validator.validateNotNull(configurationMap, "Configuration map for PersistenceManagerFactory should not be null");
		Validator.validateNotEmpty(configurationMap, "Configuration map for PersistenceManagerFactory should not be empty");

		entityPackages = argumentExtractor.initEntityPackages(configurationMap);
		configContext = parseConfiguration(configurationMap, argumentExtractor);
		eventInterceptors = argumentExtractor.initEventInterceptor(configurationMap);
	}

	protected boolean bootstrap() {
		log.info("Bootstraping Achilles PersistenceManagerFactory ");

		boolean hasSimpleCounter = false;
		try {
			hasSimpleCounter = discoverEntities();
			addEventInterceptorsToEntityMetas();
		} catch (Exception e) {
			throw new AchillesException("Exception during entity parsing : " + e.getMessage(), e);
		}

		return hasSimpleCounter;
	}

	protected void addEventInterceptorsToEntityMetas() {
		for (EventInterceptor<?> eventInterceptor : eventInterceptors) {
			String entityClassName = null;

			for (Type type : eventInterceptor.getClass().getGenericInterfaces()) {

				if (isEventInterceptorInterface(type)) {
					entityClassName = getGenericParameter(type);
				}

			}
			Class<?> entity = ReflectionUtils.forName(entityClassName, getClass().getClassLoader());
			EntityMeta entityMeta = entityMetaMap.get(entity);

			Validator.validateBeanMappingTrue(entityMeta != null, "The entity %s not found", entityClassName);

			entityMeta.addInterceptor(eventInterceptor);

		}

	}

	private String getGenericParameter(Type type) {
		;
		return StringUtils.removeStart(((ParameterizedType) type).getActualTypeArguments()[0].toString(), "class ");
	}

	private boolean isEventInterceptorInterface(Type type) {
		return (type instanceof ParameterizedType)
				&& EventInterceptor.class.getName().equals(StringUtils.removeStart(((ParameterizedType) type).getRawType().toString(), "interface "));
	}

	protected boolean discoverEntities() throws ClassNotFoundException, IOException {
		log.info("Start discovery of entities, searching in packages '{}'", StringUtils.join(entityPackages, ","));

		List<Class<?>> entities = entityExplorer.discoverEntities(entityPackages);
		boolean hasSimpleCounter = false;
		for (Class<?> entityClass : entities) {
			EntityParsingContext context = new EntityParsingContext(configContext, entityClass);

			EntityMeta entityMeta = entityParser.parseEntity(context);
			entityMetaMap.put(entityClass, entityMeta);
			hasSimpleCounter = context.getHasSimpleCounter() || hasSimpleCounter;
		}

		return hasSimpleCounter;
	}

	protected abstract AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(Map<String, Object> configurationMap,
			ArgumentExtractor argumentExtractor);

	protected ConfigurationContext parseConfiguration(Map<String, Object> configurationMap, ArgumentExtractor argumentExtractor) {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setForceColumnFamilyCreation(argumentExtractor.initForceCFCreation(configurationMap));
		configContext.setConsistencyPolicy(initConsistencyLevelPolicy(configurationMap, argumentExtractor));
		configContext.setObjectMapperFactory(argumentExtractor.initObjectMapperFactory(configurationMap));

		return configContext;
	}

	protected void setEntityPackages(List<String> entityPackages) {
		this.entityPackages = entityPackages;
	}

	protected void setEntityParser(EntityParser achillesEntityParser) {
		this.entityParser = achillesEntityParser;
	}

	protected void setEntityExplorer(EntityExplorer achillesEntityExplorer) {
		this.entityExplorer = achillesEntityExplorer;
	}

	protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap) {
		this.entityMetaMap = entityMetaMap;
	}

	protected void setConfigContext(ConfigurationContext configContext) {
		this.configContext = configContext;
	}

	public void setEventInterceptors(List<EventInterceptor<?>> eventInterceptors) {
		this.eventInterceptors = eventInterceptors;
	}
}
