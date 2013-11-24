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

package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.DEFAULT_LEVEL;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_CF_CREATION_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY_PARAM;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_PARAM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import info.archinnov.achilles.interceptor.EventInterceptor;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;

public abstract class ArgumentExtractor {

	public List<String> initEntityPackages(Map<String, Object> configurationMap) {
		List<String> entityPackages = new ArrayList<String>();
		String entityPackagesParameter = (String) configurationMap
				.get(ENTITY_PACKAGES_PARAM);
		if (StringUtils.isNotBlank(entityPackagesParameter)) {
			entityPackages = Arrays.asList(StringUtils.split(
					entityPackagesParameter, ","));
		}

		return entityPackages;
	}

	public boolean initForceCFCreation(Map<String, Object> configurationMap) {
		Boolean forceColumnFamilyCreation = (Boolean) configurationMap
				.get(FORCE_CF_CREATION_PARAM);
		if (forceColumnFamilyCreation != null) {
			return forceColumnFamilyCreation;
		} else {
			return false;
		}
	}

	public ObjectMapperFactory initObjectMapperFactory(
			Map<String, Object> configurationMap) {
		ObjectMapperFactory objectMapperFactory = (ObjectMapperFactory) configurationMap
				.get(OBJECT_MAPPER_FACTORY_PARAM);
		if (objectMapperFactory == null) {
			ObjectMapper mapper = (ObjectMapper) configurationMap
					.get(OBJECT_MAPPER_PARAM);
			if (mapper != null) {
				objectMapperFactory = factoryFromMapper(mapper);
			} else {
				objectMapperFactory = new DefaultObjectMapperFactory();
			}
		}

		return objectMapperFactory;
	}

	protected static ObjectMapperFactory factoryFromMapper(
			final ObjectMapper mapper) {
		return new ObjectMapperFactory() {
			@Override
			public <T> ObjectMapper getMapper(Class<T> type) {
				return mapper;
			}
		};
	}

	public ConsistencyLevel initDefaultReadConsistencyLevel(
			Map<String, Object> configMap) {
		String defaultReadLevel = (String) configMap
				.get(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultReadLevel);
	}

	public ConsistencyLevel initDefaultWriteConsistencyLevel(
			Map<String, Object> configMap) {
		String defaultWriteLevel = (String) configMap
				.get(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM);
		return parseConsistencyLevelOrGetDefault(defaultWriteLevel);
	}

	public Map<String, ConsistencyLevel> initReadConsistencyMap(
			Map<String, Object> configMap) {
		@SuppressWarnings("unchecked")
		Map<String, String> readConsistencyMap = (Map<String, String>) configMap
				.get(CONSISTENCY_LEVEL_READ_MAP_PARAM);

		return parseConsistencyLevelMap(readConsistencyMap);
	}

	public Map<String, ConsistencyLevel> initWriteConsistencyMap(
			Map<String, Object> configMap) {
		@SuppressWarnings("unchecked")
		Map<String, String> writeConsistencyMap = (Map<String, String>) configMap
				.get(CONSISTENCY_LEVEL_WRITE_MAP_PARAM);

		return parseConsistencyLevelMap(writeConsistencyMap);
	}

	private Map<String, ConsistencyLevel> parseConsistencyLevelMap(
			Map<String, String> consistencyLevelMap) {
		Map<String, ConsistencyLevel> map = new HashMap<String, ConsistencyLevel>();
		if (consistencyLevelMap != null && !consistencyLevelMap.isEmpty()) {
			for (Entry<String, String> entry : consistencyLevelMap.entrySet()) {
				map.put(entry.getKey(),
						parseConsistencyLevelOrGetDefault(entry.getValue()));
			}
		}

		return map;
	}

	private ConsistencyLevel parseConsistencyLevelOrGetDefault(
			String consistencyLevel) {
		ConsistencyLevel level = DEFAULT_LEVEL;
		if (StringUtils.isNotBlank(consistencyLevel)) {
			try {
				level = ConsistencyLevel.valueOf(consistencyLevel);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("'" + consistencyLevel
						+ "' is not a valid Consistency Level");
			}
		}
		return level;
	}

	public List<EventInterceptor<? extends Object>> initEventInterceptor(
			Map<String, Object> configurationMap) {

		@SuppressWarnings("unchecked")
		List<EventInterceptor<? extends Object>> eventInterceptors = (List<EventInterceptor<? extends Object>>) configurationMap
				.get(EVENT_INTERCEPTORS);
		if (eventInterceptors == null) {

			eventInterceptors = new ArrayList<EventInterceptor<? extends Object>>();
		}
		return eventInterceptors;
	}
}
