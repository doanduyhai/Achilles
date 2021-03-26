/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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
package info.archinnov.achilles.internals.context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import javax.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;

import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.interceptor.DefaultPostLoadBeanValidationInterceptor;
import info.archinnov.achilles.internals.interceptor.DefaultPreMutateBeanValidationInterceptor;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.type.interceptor.Interceptor;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

public class ConfigurationContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationContext.class);

    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_ONE;
    public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_SERIAL;

    private boolean forceSchemaGeneration;
    private boolean validateSchema = true;

    private List<Class<?>> manageEntities;

    private JacksonMapperFactory jacksonMapperFactory;

    private Optional<ConsistencyLevel> defaultReadConsistencyLevel;
    private Optional<ConsistencyLevel> defaultWriteConsistencyLevel;
    private Optional<ConsistencyLevel> defaultSerialConsistencyLevel;

    private Map<String, ConsistencyLevel> readConsistencyLevelMap = new HashMap<>();
    private Map<String, ConsistencyLevel> writeConsistencyLevelMap = new HashMap<>();
    private Map<String, ConsistencyLevel> serialConsistencyLevelMap = new HashMap<>();

    private Validator beanValidator;
    private DefaultPreMutateBeanValidationInterceptor preMutateBeanValidationInterceptor;
    private Optional<DefaultPostLoadBeanValidationInterceptor> postLoadBeanValidationInterceptor = Optional.empty();

    private List<Interceptor<?>> interceptors;

    private int preparedStatementLRUCacheSize;

    private InsertStrategy globalInsertStrategy;
    private NamingStrategy globalNamingStrategy;

    private Optional<String> currentKeyspace = Optional.empty();

    private ExecutorService executorService;
    private boolean providedExecutorService;

    private BeanFactory defaultBeanFactory;

    private Session session;
    private boolean providedSession = false;

    private Optional<SchemaNameProvider> schemaNameProvider = Optional.empty();

    private StatementsCache statementsCache;

    private Map<CodecSignature<?,?>, Codec<?, ?>> runtimeCodecs = new HashMap<>();

    private Integer DMLResultsDisplaySize;

    public boolean isForceSchemaGeneration() {
        return forceSchemaGeneration;
    }

    public void setForceSchemaGeneration(boolean forceSchemaGeneration) {
        this.forceSchemaGeneration = forceSchemaGeneration;
    }

    public boolean isValidateSchema() {
        return validateSchema;
    }

    public void setValidateSchema(boolean validateSchema) {
        this.validateSchema = validateSchema;
    }

    public List<Class<?>> getManageEntities() {
        return manageEntities;
    }

    public void setManageEntities(List<Class<?>> manageEntities) {
        this.manageEntities = manageEntities;
    }

    public JacksonMapperFactory getJacksonMapperFactory() {
        return jacksonMapperFactory;
    }

    public void setJacksonMapperFactory(JacksonMapperFactory jacksonMapperFactory) {
        this.jacksonMapperFactory = jacksonMapperFactory;
    }

    public Optional<ConsistencyLevel> getDefaultReadConsistencyLevel() {
        return defaultReadConsistencyLevel;
    }

    public void setDefaultReadConsistencyLevel(Optional<ConsistencyLevel> defaultReadConsistencyLevel) {
        this.defaultReadConsistencyLevel = defaultReadConsistencyLevel;
    }

    public Optional<ConsistencyLevel> getDefaultWriteConsistencyLevel() {
        return defaultWriteConsistencyLevel;
    }

    public void setDefaultWriteConsistencyLevel(Optional<ConsistencyLevel> defaultWriteConsistencyLevel) {
        this.defaultWriteConsistencyLevel = defaultWriteConsistencyLevel;
    }

    public Optional<ConsistencyLevel> getDefaultSerialConsistencyLevel() {
        return defaultSerialConsistencyLevel;
    }

    public void setDefaultSerialConsistencyLevel(Optional<ConsistencyLevel> defaultSerialConsistencyLevel) {
        this.defaultSerialConsistencyLevel = defaultSerialConsistencyLevel;
    }

    public Validator getBeanValidator() {
        return beanValidator;
    }

    public void setBeanValidator(Validator beanValidator) {
        this.beanValidator = beanValidator;
        this.preMutateBeanValidationInterceptor = new DefaultPreMutateBeanValidationInterceptor(beanValidator);
    }

    public void setPostLoadBeanValidationEnabled(boolean postLoadBeanValidationEnabled) {
        if (postLoadBeanValidationEnabled) {
            this.postLoadBeanValidationInterceptor = Optional.of(new DefaultPostLoadBeanValidationInterceptor(this.beanValidator));
        }
    }

    public List<Interceptor<?>> getInterceptors() {
        return interceptors;
    }

    public void setInterceptors(List<Interceptor<?>> interceptors) {
        this.interceptors = interceptors;
    }

    public int getPreparedStatementLRUCacheSize() {
        return preparedStatementLRUCacheSize;
    }

    public void setPreparedStatementLRUCacheSize(int preparedStatementLRUCacheSize) {
        this.preparedStatementLRUCacheSize = preparedStatementLRUCacheSize;
    }

    public InsertStrategy getGlobalInsertStrategy() {
        return globalInsertStrategy;
    }

    public void setGlobalInsertStrategy(InsertStrategy globalInsertStrategy) {
        this.globalInsertStrategy = globalInsertStrategy;
    }

    public NamingStrategy getGlobalNamingStrategy() {
        return globalNamingStrategy;
    }

    public void setGlobalNamingStrategy(NamingStrategy globalNamingStrategy) {
        this.globalNamingStrategy = globalNamingStrategy;
    }

    public boolean isClassConstrained(Class<?> clazz) {
        if (beanValidator != null) {
            return beanValidator.getConstraintsForClass(clazz).isBeanConstrained();
        } else {
            return false;
        }
    }

    public ConsistencyLevel getReadConsistencyLevelForTable(String tableName) {
        return readConsistencyLevelMap.get(tableName);
    }

    public ConsistencyLevel getWriteConsistencyLevelForTable(String tableName) {
        return writeConsistencyLevelMap.get(tableName);
    }

    public ConsistencyLevel getSerialConsistencyLevelForTable(String tableName) {
        return serialConsistencyLevelMap.get(tableName);
    }

    public ObjectMapper getMapperFor(Class<?> type) {
        return jacksonMapperFactory.getMapper(type);
    }

    public void setReadConsistencyLevelMap(Map<String, ConsistencyLevel> readConsistencyLevelMap) {
        this.readConsistencyLevelMap = readConsistencyLevelMap;
    }

    public void setWriteConsistencyLevelMap(Map<String, ConsistencyLevel> writeConsistencyLevelMap) {
        this.writeConsistencyLevelMap = writeConsistencyLevelMap;
    }

    public void setSerialConsistencyLevelMap(Map<String, ConsistencyLevel> serialConsistencyLevelMap) {
        this.serialConsistencyLevelMap = serialConsistencyLevelMap;
    }

    public Optional<String> getCurrentKeyspace() {
        return currentKeyspace;
    }

    public void setCurrentKeyspace(Optional<String> currentKeyspace) {
        this.currentKeyspace = currentKeyspace;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public BeanFactory getDefaultBeanFactory() {
        return defaultBeanFactory;
    }

    public void setDefaultBeanFactory(BeanFactory defaultBeanFactory) {
        this.defaultBeanFactory = defaultBeanFactory;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Optional<SchemaNameProvider> getSchemaNameProvider() {
        return schemaNameProvider;
    }

    public void setSchemaNameProvider(Optional<SchemaNameProvider> schemaNameProvider) {
        this.schemaNameProvider = schemaNameProvider;
    }

    public boolean isProvidedSession() {
        return providedSession;
    }

    public void setProvidedSession(boolean providedSession) {
        this.providedSession = providedSession;
    }

    public boolean isProvidedExecutorService() {
        return providedExecutorService;
    }

    public void setProvidedExecutorService(boolean providedExecutorService) {
        this.providedExecutorService = providedExecutorService;
    }

    public void injectDependencies(TupleTypeFactory tupleTypeFactory, UserTypeFactory userTypeFactory, AbstractEntityProperty<?> entityProperty) {
        LOGGER.debug("Start injecting dependencies to meta classes");

        final Class<?> entityClass = entityProperty.entityClass;

        if (currentKeyspace.isPresent()) {
            LOGGER.debug("Injecting current global keyspace");
            entityProperty.injectKeyspace(currentKeyspace.get());
        }

        if (schemaNameProvider.isPresent()) {
            LOGGER.debug("Injecting schema name provider");
            entityProperty.inject(schemaNameProvider.get());
        }

        LOGGER.debug("Injecting default bean factory");
        entityProperty.inject(defaultBeanFactory);

        LOGGER.debug("Injecting Jackson mapper");
        entityProperty.inject(jacksonMapperFactory.getMapper(entityClass));

        LOGGER.debug("Injecting global Insert strategy");
        entityProperty.inject(globalInsertStrategy);

        if (!interceptors.isEmpty()) {
            LOGGER.debug("Injecting bean interceptors");
            interceptors.stream()
                    .filter(x -> x.acceptEntity(entityClass))
                    .map(x -> (Interceptor) x)
                    .forEach(entityProperty.interceptors::add);
        }

        // Adding PreMutate Bean validator as the LAST interceptor
        if (beanValidator != null && isClassConstrained(entityClass)) {
            LOGGER.debug("Injecting Bean validator (JSR 303)");
            if (entityProperty.isTable()) {
                entityProperty.interceptors.add((Interceptor) preMutateBeanValidationInterceptor);
            }

            // Add PostLoad interceptor as the FIRST interceptor
            if (postLoadBeanValidationInterceptor.isPresent()) {
                entityProperty.interceptors.add(0, (Interceptor) postLoadBeanValidationInterceptor.get());
            }
        }

        LOGGER.debug("Injecting global consistency levels");
        entityProperty.injectConsistencyLevels(session, this);

        LOGGER.debug("Injecting runtime codecs");
        entityProperty.injectRuntimeCodecs(runtimeCodecs);

        LOGGER.debug("Injecting user type factory and tuple type factory");
        entityProperty.inject(userTypeFactory, tupleTypeFactory);

    }


    public StatementsCache getStatementsCache() {
        return statementsCache;
    }

    public void setStatementsCache(StatementsCache statementsCache) {
        this.statementsCache = statementsCache;
    }

    public Map<CodecSignature<?, ?>, Codec<?, ?>> getRuntimeCodecs() {
        return runtimeCodecs;
    }

    public void setRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        this.runtimeCodecs = runtimeCodecs;
    }

    public Integer getDMLResultsDisplaySize() {
        return DMLResultsDisplaySize;
    }

    public void setDMLResultsDisplaySize(Integer DMLResultsDisplaySize) {
        this.DMLResultsDisplaySize = DMLResultsDisplaySize;
    }
}
