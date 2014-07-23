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
package info.archinnov.achilles.internal.context;

import java.util.HashMap;
import java.util.Map;
import javax.validation.Validator;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.interceptor.DefaultBeanValidationInterceptor;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;

public class ConfigurationContext {
    private boolean forceColumnFamilyCreation;

    private boolean enableSchemaUpdate;

    private Map<String, Boolean> enableSchemaUpdateForTables;

    private JacksonMapperFactory jacksonMapperFactory;

    private ConsistencyLevel defaultReadConsistencyLevel;

    private ConsistencyLevel defaultWriteConsistencyLevel;

    private Map<String, ConsistencyLevel> readConsistencyLevelMap = new HashMap<>();

    private Map<String, ConsistencyLevel> writeConsistencyLevelMap = new HashMap<>();

    private Validator beanValidator;

    private DefaultBeanValidationInterceptor beanValidationInterceptor;

    private int preparedStatementLRUCacheSize = 10000;

    private InsertStrategy insertStrategy;

    private ClassLoader OSGIClassLoader;

    public boolean isForceColumnFamilyCreation() {
        return forceColumnFamilyCreation;
    }

    public void setForceColumnFamilyCreation(boolean forceColumnFamilyCreation) {
        this.forceColumnFamilyCreation = forceColumnFamilyCreation;
    }

    public boolean isEnableSchemaUpdate() {
        return enableSchemaUpdate;
    }

    public void setEnableSchemaUpdateForTables(Map<String, Boolean> enableSchemaUpdateForTables) {
        this.enableSchemaUpdateForTables = enableSchemaUpdateForTables;
    }

    public Map<String, Boolean> getEnableSchemaUpdateForTables() {
        return enableSchemaUpdateForTables;
    }

    public void setEnableSchemaUpdate(boolean enableSchemaUpdate) {
        this.enableSchemaUpdate = enableSchemaUpdate;
    }

    public JacksonMapperFactory getJacksonMapperFactory() {
        return jacksonMapperFactory;
    }

    public void setJacksonMapperFactory(JacksonMapperFactory jacksonMapperFactory) {
        this.jacksonMapperFactory = jacksonMapperFactory;
    }

    public ConsistencyLevel getDefaultReadConsistencyLevel() {
        return defaultReadConsistencyLevel;
    }

    public void setDefaultReadConsistencyLevel(ConsistencyLevel defaultReadConsistencyLevel) {
        this.defaultReadConsistencyLevel = defaultReadConsistencyLevel;
    }

    public ConsistencyLevel getDefaultWriteConsistencyLevel() {
        return defaultWriteConsistencyLevel;
    }

    public void setDefaultWriteConsistencyLevel(ConsistencyLevel defaultWriteConsistencyLevel) {
        this.defaultWriteConsistencyLevel = defaultWriteConsistencyLevel;
    }

    public Validator getBeanValidator() {
        return beanValidator;
    }

    public void setBeanValidator(Validator beanValidator) {
        this.beanValidator = beanValidator;
    }

    public int getPreparedStatementLRUCacheSize() {
        return preparedStatementLRUCacheSize;
    }

    public void setPreparedStatementLRUCacheSize(int preparedStatementLRUCacheSize) {
        this.preparedStatementLRUCacheSize = preparedStatementLRUCacheSize;
    }

    public InsertStrategy getInsertStrategy() {
        return insertStrategy;
    }

    public void setInsertStrategy(InsertStrategy insertStrategy) {
        this.insertStrategy = insertStrategy;
    }

    public void setOSGIClassLoader(ClassLoader OSGIClassLoader) {
        this.OSGIClassLoader = OSGIClassLoader;
    }

    public boolean isClassConstrained(Class<?> clazz) {
        if (beanValidator != null) {
            return beanValidator.getConstraintsForClass(clazz).isBeanConstrained();
        } else {
            return false;
        }
    }

    public void addBeanValidationInterceptor(EntityMeta meta) {
        if (beanValidationInterceptor == null) {
            beanValidationInterceptor = new DefaultBeanValidationInterceptor(beanValidator);
        }
        meta.addInterceptor(beanValidationInterceptor);
    }

    public ConsistencyLevel getReadConsistencyLevelForTable(String tableName) {
        return readConsistencyLevelMap.get(tableName);
    }

    public ConsistencyLevel getWriteConsistencyLevelForTable(String tableName) {
        return writeConsistencyLevelMap.get(tableName);
    }


    public ObjectMapper getMapperFor(Class<?> type) {
        return jacksonMapperFactory.getMapper(type);
    }

    public ClassLoader selectClassLoader(Class<?> entityClass) {
        return OSGIClassLoader != null ? OSGIClassLoader : entityClass.getClassLoader();
    }

    public ClassLoader selectClassLoader() {
        return OSGIClassLoader != null ? OSGIClassLoader : this.getClass().getClassLoader();
    }

    public void setReadConsistencyLevelMap(Map<String, ConsistencyLevel> readConsistencyLevelMap) {
        this.readConsistencyLevelMap = readConsistencyLevelMap;
    }

    public void setWriteConsistencyLevelMap(Map<String, ConsistencyLevel> writeConsistencyLevelMap) {
        this.writeConsistencyLevelMap = writeConsistencyLevelMap;
    }
}
