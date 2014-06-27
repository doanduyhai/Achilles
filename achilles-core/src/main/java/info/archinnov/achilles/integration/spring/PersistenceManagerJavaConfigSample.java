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
package info.archinnov.achilles.integration.spring;

import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_ENABLE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_VALIDATOR;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.INSERT_STRATEGY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OSGI_CLASS_LOADER;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PROXIES_WARM_UP_DISABLED;
import static info.archinnov.achilles.persistence.PersistenceManagerFactory.PersistenceManagerFactoryBuilder;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.split;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.validation.Validator;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.type.InsertStrategy;

@Configuration
public class PersistenceManagerJavaConfigSample {

    @Value("#{cassandraProperties['achilles.entity.packages']}")
    private String entityPackages;

    @Autowired
    private List<Class<?>> entityList;

    @Value("#{cassandraProperties['achilles.cassandra.keyspace.name']}")
    private String keyspaceName;

    @Autowired
    private Cluster cluster;

    @Autowired
    private Session session;

    @Autowired
    private ObjectMapperFactory objecMapperFactory;

    @Autowired
    private List<Interceptor<?>> eventInterceptors;

    @Value("#{cassandraProperties['achilles.consistency.read.default']}")
    private String consistencyLevelReadDefault;

    @Value("#{cassandraProperties['achilles.consistency.write.default']}")
    private String consistencyLevelWriteDefault;

    @Value("#{cassandraProperties['achilles.consistency.read.map']}")
    private String consistencyLevelReadMap;

    @Value("#{cassandraProperties['achilles.consistency.write.map']}")
    private String consistencyLevelWriteMap;

    @Value("#{cassandraProperties['achilles.ddl.force.table.creation']}")
    private boolean forceTableCreation;

    @Value("#{cassandraProperties['achilles.bean.validation.enable']}")
    private boolean enableBeanValidation;

    @Autowired
    private Validator validator;

    private PersistenceManagerFactory pmf;

    @Value("#{cassandraProperties['achilles.prepared.statements.cache.size']}")
    private int preparedStatementsCacheSize;

    @Value("#{cassandraProperties['achilles.proxies.warm.up.disabled']}")
    private boolean disableProxiesWarmUp;

    @Value("#{cassandraProperties['achilles.batch.force.statements.ordering']}")
    private boolean forceBatchStatementsOrdering;

    @Autowired
    private InsertStrategy insertStrategy;

    @Autowired
    private ClassLoader osgiClassLoader;

    @PostConstruct
    public void initialize() {
        Map<ConfigurationParameters, Object> configMap = extractConfigParams();
        pmf = PersistenceManagerFactoryBuilder.build(cluster, configMap);
    }

    @Bean
    public PersistenceManager getPersistenceManager() {
        return pmf.createPersistenceManager();
    }

    private Map<ConfigurationParameters, Object> extractConfigParams() {
        Map<ConfigurationParameters, Object> configMap = new HashMap<>();
        configMap.put(ENTITY_PACKAGES, entityPackages);
        configMap.put(ENTITIES_LIST, entityList);

        if (session != null) {
            configMap.put(NATIVE_SESSION, session);
        }

        configMap.put(KEYSPACE_NAME, keyspaceName);
        configMap.put(OBJECT_MAPPER_FACTORY, objecMapperFactory);

        if (isNotBlank(consistencyLevelReadDefault)) {
            configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT, consistencyLevelReadDefault);
        }
        if (isNotBlank(consistencyLevelWriteDefault)) {
            configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT, consistencyLevelWriteDefault);
        }

        if (isNotBlank(consistencyLevelReadMap)) {
            configMap.put(CONSISTENCY_LEVEL_READ_MAP, extractConsistencyMap(consistencyLevelReadMap));
        }
        if (isNotBlank(consistencyLevelWriteMap)) {
            configMap.put(CONSISTENCY_LEVEL_WRITE_MAP, extractConsistencyMap(consistencyLevelWriteMap));
        }

        configMap.put(EVENT_INTERCEPTORS, eventInterceptors);

        configMap.put(FORCE_TABLE_CREATION, forceTableCreation);

        if (enableBeanValidation) {
            configMap.put(BEAN_VALIDATION_ENABLE, enableBeanValidation);
        }

        if (validator != null) {
            configMap.put(BEAN_VALIDATION_VALIDATOR, validator);
        }

        configMap.put(PREPARED_STATEMENTS_CACHE_SIZE, preparedStatementsCacheSize);
        configMap.put(PROXIES_WARM_UP_DISABLED, disableProxiesWarmUp);

        if (insertStrategy != null) {
            configMap.put(INSERT_STRATEGY, insertStrategy);
        }

        if (osgiClassLoader != null) {
            configMap.put(OSGI_CLASS_LOADER, osgiClassLoader);
        }

        return configMap;
    }

    private Map<String, String> extractConsistencyMap(String consistencyMapProperty) {
        Map<String, String> consistencyMap = new HashMap<>();

        for (String entry : split(consistencyMapProperty, ",")) {
            String[] entryValue = StringUtils.split(entry, ":");
            assert entryValue.length == 2 : "Invalid map value : " + entry + " for the property : "
                    + consistencyMapProperty;
            consistencyMap.put(entryValue[0], entryValue[1]);
        }
        return consistencyMap;
    }
}
