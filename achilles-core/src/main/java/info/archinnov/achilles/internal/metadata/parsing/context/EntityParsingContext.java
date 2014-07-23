/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.parsing.context;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.Pair;


public class EntityParsingContext {
    private ConfigurationContext configContext;
    private Boolean hasCounter = false;

    private Map<String, PropertyMeta> propertyMetas = new HashMap<>();
    private List<PropertyMeta> counterMetas = new ArrayList<>();
    private Class<?> currentEntityClass;
    private ObjectMapper currentObjectMapper;
    private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;

    public EntityParsingContext(ConfigurationContext configContext, Class<?> currentEntityClass) {
        this.configContext = configContext;
        this.currentEntityClass = currentEntityClass;
    }

    public PropertyParsingContext newPropertyContext(Field currentField) {
        return new PropertyParsingContext(this, currentField);
    }

    public Class<?> getCurrentEntityClass() {
        return currentEntityClass;
    }

    public Map<String, PropertyMeta> getPropertyMetas() {
        return propertyMetas;
    }


    public Boolean hasSimpleCounter() {
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

    public void setCurrentConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels) {
        this.currentConsistencyLevels = currentConsistencyLevels;
    }

    public List<PropertyMeta> getCounterMetas() {
        return counterMetas;
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels() {
        return currentConsistencyLevels;
    }

    public JacksonMapperFactory getObjectMapperFactory() {
        return configContext.getJacksonMapperFactory();
    }


    public boolean isSchemaUpdateEnabled(String columnFamilyName) {
        Map<String, Boolean> columnFamilyUpdateMap = configContext.getEnableSchemaUpdateForTables();
        if (columnFamilyUpdateMap.containsKey(columnFamilyName)) {
            return columnFamilyUpdateMap.get(columnFamilyName);
        }
        return configContext.isEnableSchemaUpdate();
    }

    public InsertStrategy getDefaultInsertStrategy() {
        return configContext.getInsertStrategy();
    }

    public ConfigurationContext getConfigContext() {
        return configContext;
    }
}
