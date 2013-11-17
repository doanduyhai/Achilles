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
package info.archinnov.achilles.entity.parsing.context;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import info.archinnov.achilles.type.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;

public class EntityParsingContext {
	private ConfigurationContext configContext;
	private Boolean hasCounter = false;

	private Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
	private List<PropertyMeta> counterMetas = new ArrayList<PropertyMeta>();
	private Class<?> currentEntityClass;
	private ObjectMapper currentObjectMapper;
	private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;
	private boolean clusteredEntity = false;
	private String currentColumnFamilyName;

	public EntityParsingContext(//
			ConfigurationContext configContext, //
			Class<?> currentEntityClass) {
		this.configContext = configContext;
		this.currentEntityClass = currentEntityClass;
	}

	public EntityParsingContext( //
			ConfigurationContext configContext) {
		this.configContext = configContext;
	}

	public PropertyParsingContext newPropertyContext(Field currentField) {
		return new PropertyParsingContext(this, currentField);
	}

	public boolean isThriftImpl() {
		return configContext.getImpl() == Impl.THRIFT;
	}

	public Class<?> getCurrentEntityClass() {
		return currentEntityClass;
	}

	public Map<String, PropertyMeta> getPropertyMetas() {
		return propertyMetas;
	}

	public void setPropertyMetas(Map<String, PropertyMeta> propertyMetas) {
		this.propertyMetas = propertyMetas;
	}

	public Boolean getHasSimpleCounter() {
		return hasCounter;
	}

	public void setHasSimpleCounter(Boolean hasCounter) {
		this.hasCounter = hasCounter;
	}

	public ObjectMapper getCurrentObjectMapper() {
		return currentObjectMapper;
	}

	public void setCurrentObjectMapper(ObjectMapper currentObjectMapper) {
		this.currentObjectMapper = currentObjectMapper;
	}

	public boolean isClusteredEntity() {
		return clusteredEntity;
	}

	public void setClusteredEntity(boolean wideRow) {
		this.clusteredEntity = wideRow;
	}

	public void setCurrentConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels) {
		this.currentConsistencyLevels = currentConsistencyLevels;
	}

	public List<PropertyMeta> getCounterMetas() {
		return counterMetas;
	}

	public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels() {
		return currentConsistencyLevels;
	}

	public String getCurrentColumnFamilyName() {
		return currentColumnFamilyName;
	}

	public void setCurrentColumnFamilyName(String currentColumnFamilyName) {
		this.currentColumnFamilyName = currentColumnFamilyName;
	}

	public ObjectMapperFactory getObjectMapperFactory() {
		return configContext.getObjectMapperFactory();
	}

	public AchillesConsistencyLevelPolicy getConfigurableCLPolicy() {
		return configContext.getConsistencyPolicy();
	}
}
