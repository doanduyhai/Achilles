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
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.EntityIntrospector;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.NamingStrategy;
import info.archinnov.achilles.type.Pair;


public class EntityParsingContext {

    private EntityIntrospector introspector = new EntityIntrospector();

    private ConfigurationContext configContext;
    private Boolean hasCounter = false;
    private Map<String, PropertyMeta> propertyMetas = new HashMap<>();
    private List<PropertyMeta> counterMetas = new ArrayList<>();
    private Class<?> currentEntityClass;
    private ObjectMapper currentObjectMapper;
    private Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels;
    private NamingStrategy namingStrategy;
    private String keyspaceName;
    private String tableName;

    public EntityParsingContext(ConfigurationContext configContext, Class<?> currentEntityClass) {
        this.configContext = configContext;
        this.currentEntityClass = currentEntityClass;
        this.namingStrategy = introspector.determineClassNamingStrategy(configContext, currentEntityClass);
        this.keyspaceName = introspector.inferKeyspaceName(currentEntityClass, configContext.getCurrentKeyspace(), namingStrategy);
        this.tableName = introspector.inferTableName(currentEntityClass, currentEntityClass.getName(), namingStrategy);
        this.currentConsistencyLevels = introspector.findConsistencyLevels(currentEntityClass, tableName, configContext);
    }

    private EntityParsingContext(ConfigurationContext configContext, Class<?> currentEntityClass, ObjectMapper currentObjectMapper, NamingStrategy namingStrategy, String keyspaceName, String tableName, Pair<ConsistencyLevel, ConsistencyLevel> currentConsistencyLevels) {
        this.configContext = configContext;
        this.currentEntityClass = currentEntityClass;
        this.currentObjectMapper = currentObjectMapper;
        this.namingStrategy = namingStrategy;
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.currentConsistencyLevels = currentConsistencyLevels;
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


    public boolean isSchemaUpdateEnabled(String keyspaceName,String tableName) {
        String qualifiedTableName = keyspaceName + "." + tableName;
        Map<String, Boolean> columnFamilyUpdateMap = configContext.getEnableSchemaUpdateForTables();
        if (columnFamilyUpdateMap.containsKey(qualifiedTableName)) {
            return columnFamilyUpdateMap.get(qualifiedTableName);
        }
        return configContext.isEnableSchemaUpdate();
    }

    public InsertStrategy getDefaultInsertStrategy() {
        return configContext.getGlobalInsertStrategy();
    }

    public ConfigurationContext getConfigContext() {
        return configContext;
    }

    public EntityParsingContext duplicateForClass(Class<?> entityClass) {
        return new EntityParsingContext(configContext, entityClass, currentObjectMapper, namingStrategy, keyspaceName, tableName, currentConsistencyLevels);
    }

    public Optional<String> getCurrentKeyspaceName() {
        return configContext.getCurrentKeyspace();
    }

    public NamingStrategy getNamingStrategy() {
        return namingStrategy;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public String getTableName() {
        return tableName;
    }
}
