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

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.map.ObjectMapper;

public class PropertyParsingContext {
	private EntityParsingContext context;
	private Field currentField;
	private String currentPropertyName;
	private boolean isCustomConsistencyLevels;
	private boolean primaryKey = false;
	private boolean embeddedId = false;

	public PropertyParsingContext(EntityParsingContext context, //
			Field currentField) {
		this.context = context;
		this.currentField = currentField;
	}

	public ObjectMapper getCurrentObjectMapper() {
		return context.getCurrentObjectMapper();
	}

	public Map<String, PropertyMeta> getPropertyMetas() {
		return context.getPropertyMetas();
	}

	public Class<?> getCurrentEntityClass() {
		return context.getCurrentEntityClass();
	}

	public Field getCurrentField() {
		return currentField;
	}

	public String getCurrentPropertyName() {
		return currentPropertyName;
	}

	public void setCurrentPropertyName(String currentPropertyName) {
		this.currentPropertyName = currentPropertyName;
	}

	public List<PropertyMeta> getCounterMetas() {
		return context.getCounterMetas();
	}

	public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels() {
		return context.getCurrentConsistencyLevels();
	}

	public AchillesConsistencyLevelPolicy getConfigurableCLPolicy() {
		return context.getConfigurableCLPolicy();
	}

	public String getCurrentColumnFamilyName() {
		return context.getCurrentColumnFamilyName();
	}

	public boolean isCustomConsistencyLevels() {
		return isCustomConsistencyLevels;
	}

	public void setCustomConsistencyLevels(boolean isCustomConsistencyLevels) {
		this.isCustomConsistencyLevels = isCustomConsistencyLevels;
	}

	public void hasSimpleCounterType() {
		context.setHasSimpleCounter(true);
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}

	public boolean isEmbeddedId() {
		return embeddedId;
	}

	public void isEmbeddedId(boolean embeddedId) {
		if (embeddedId) {
			this.primaryKey = true;
		}
		this.embeddedId = embeddedId;
	}

	public boolean isClusteredEntity() {
		return context.isClusteredEntity();
	}
}
