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
package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ArgumentExtractor.DEFAULT_LRU_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_ENABLE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_VALIDATOR;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_DEFAULT;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_DEFAULT;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENABLE_SCHEMA_UPDATE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENABLE_SCHEMA_UPDATE_FOR_TABLES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_BATCH_STATEMENTS_ORDERING;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.INSERT_STRATEGY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.configuration.ConfigurationParameters.NATIVE_SESSION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OBJECT_MAPPER_FACTORY;
import static info.archinnov.achilles.configuration.ConfigurationParameters.OSGI_CLASS_LOADER;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.PROXIES_WARM_UP_DISABLED;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.ANY;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.validation.Validator;
import org.fest.assertions.data.MapEntry;
import org.hibernate.validator.internal.engine.ValidatorImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.bean.validation.FakeValidator;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.test.more.entity.Entity3;
import info.archinnov.achilles.test.sample.entity.Entity1;
import info.archinnov.achilles.test.sample.entity.Entity2;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;

@RunWith(MockitoJUnitRunner.class)
public class ArgumentExtractorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Spy
    private ArgumentExtractor extractor = new ArgumentExtractor();

    @Mock
    private ObjectMapper mapper;

    @Mock
    private ObjectMapperFactory factory;

    @Mock
    private Cluster cluster;

    @Mock
    private Session session;

    @Mock
    private Interceptor<String> interceptor1;

    @Mock
    private Interceptor<String> interceptor2;

    private ConfigMap configMap = new ConfigMap();

    @Before
    public void setUp() {
        configMap.clear();
    }

    @Test
    public void should_init_entity_packages() throws Exception {
        configMap.put(ENTITY_PACKAGES, "info.archinnov.achilles.test.sample.entity,info.archinnov.achilles.test.more.entity");

        Collection<Class<?>> actual = extractor.initEntities(configMap, this.getClass().getClassLoader());

        assertThat(actual).containsOnly(Entity1.class, Entity2.class, Entity3.class);
    }

    @Test
    public void should_init_empty_entity_packages() throws Exception {
        Collection<Class<?>> actual = extractor.initEntities(configMap, this.getClass().getClassLoader());

        assertThat(actual).isEmpty();
    }

    @Test
    public void should_init_entities_list() {
        configMap.put(ENTITIES_LIST, Arrays.asList(Entity1.class));

        Collection<Class<?>> actual = extractor.initEntities(configMap, this.getClass().getClassLoader());

        assertThat(actual).contains(Entity1.class);
    }

    @Test
    public void should_init_empty_entities_list() {
        Collection<Class<?>> actual = extractor.initEntities(configMap, this.getClass().getClassLoader());

        assertThat(actual).isEmpty();
    }

    @Test
    public void should_init_from_packages_and_entities_list() {
        configMap.put(ENTITIES_LIST, Arrays.asList(Entity1.class));
        configMap.put(ENTITY_PACKAGES, "info.archinnov.achilles.test.more.entity");

        Collection<Class<?>> actual = extractor.initEntities(configMap, this.getClass().getClassLoader());

        assertThat(actual).containsOnly(Entity1.class, Entity3.class);
    }

    @Test
    public void should_init_forceCFCreation_to_default_value() throws Exception {
        boolean actual = extractor.initForceTableCreation(configMap);

        assertThat(actual).isFalse();
    }

    @Test
    public void should_init_forceCFCreation() throws Exception {
        configMap.put(FORCE_TABLE_CREATION, true);

        boolean actual = extractor.initForceTableCreation(configMap);

        assertThat(actual).isTrue();
    }

    @Test
    public void should_init_default_object_factory_mapper() throws Exception {
        ObjectMapperFactory actual = extractor.initObjectMapperFactory(configMap);

        assertThat(actual).isNotNull();

        ObjectMapper mapper = actual.getMapper(Integer.class);

        assertThat(mapper).isNotNull();
        assertThat(mapper.getSerializationConfig().getSerializationInclusion()).isEqualTo(JsonInclude.Include.NON_NULL);
        assertThat(mapper.getDeserializationConfig().isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)).isFalse();
        Collection<AnnotationIntrospector> ais = mapper.getSerializationConfig().getAnnotationIntrospector().allIntrospectors();

        assertThat(ais).hasSize(2);
        Iterator<AnnotationIntrospector> iterator = ais.iterator();

        assertThat(iterator.next()).isInstanceOfAny(JacksonAnnotationIntrospector.class, JaxbAnnotationIntrospector.class);
        assertThat(iterator.next()).isInstanceOfAny(JacksonAnnotationIntrospector.class, JaxbAnnotationIntrospector.class);
    }

    @Test
    public void should_init_object_mapper_factory_from_mapper() throws Exception {
        configMap.put(OBJECT_MAPPER, mapper);

        ObjectMapperFactory actual = extractor.initObjectMapperFactory(configMap);

        assertThat(actual).isNotNull();
        assertThat(actual.getMapper(Long.class)).isSameAs(mapper);
    }

    @Test
    public void should_init_force_update() throws Exception {
        configMap.put(ENABLE_SCHEMA_UPDATE, true);

        boolean actual = extractor.initForceTableUpdate(configMap);

        assertThat(actual).isTrue();
    }

    @Test
    public void should_init_force_update_map() throws Exception {
        //Given
        configMap.put(ENABLE_SCHEMA_UPDATE_FOR_TABLES, ImmutableMap.of("myTable", true));

        //When
        Map<String, Boolean> tables = extractor.initForceTableUpdateMap(configMap);

        //Then
        assertThat(tables).contains(MapEntry.entry("myTable", true));
    }

    @Test
    public void should_init_default_force_update_to_false() throws Exception {
        boolean actual = extractor.initForceTableUpdate(configMap);
        Map<String, Boolean> tables = extractor.initForceTableUpdateMap(configMap);

        assertThat(actual).isFalse();
        assertThat(tables.isEmpty());
    }

    @Test
    public void should_init_object_mapper_factory() throws Exception {
        configMap.put(OBJECT_MAPPER_FACTORY, factory);

        ObjectMapperFactory actual = extractor.initObjectMapperFactory(configMap);

        assertThat(actual).isSameAs(factory);
    }

    @Test
    public void should_init_default_read_consistency_level() throws Exception {
        configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT, "ONE");

        assertThat(extractor.initDefaultReadConsistencyLevel(configMap)).isEqualTo(ONE);
    }

    @Test
    public void should_exception_when_invalid_consistency_level() throws Exception {
        configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT, "wrong_value");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("'wrong_value' is not a valid Consistency Level");

        extractor.initDefaultReadConsistencyLevel(configMap);
    }

    @Test
    public void should_init_default_write_consistency_level() throws Exception {
        configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT, "LOCAL_QUORUM");

        assertThat(extractor.initDefaultWriteConsistencyLevel(configMap)).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_return_default_one_level_when_no_parameter() throws Exception {
        assertThat(extractor.initDefaultReadConsistencyLevel(configMap)).isEqualTo(ONE);
    }

    @Test
    public void should_init_read_consistency_level_map() throws Exception {
        configMap.put(CONSISTENCY_LEVEL_READ_MAP, ImmutableMap.of("cf1", "ONE", "cf2", "LOCAL_QUORUM"));

        Map<String, ConsistencyLevel> consistencyMap = extractor.initReadConsistencyMap(configMap);

        assertThat(consistencyMap.get("cf1")).isEqualTo(ConsistencyLevel.ONE);
        assertThat(consistencyMap.get("cf2")).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
    }

    @Test
    public void should_init_write_consistency_level_map() throws Exception {
        configMap.put(CONSISTENCY_LEVEL_WRITE_MAP, ImmutableMap.of("cf1", "THREE", "cf2", "EACH_QUORUM"));

        Map<String, ConsistencyLevel> consistencyMap = extractor.initWriteConsistencyMap(configMap);

        assertThat(consistencyMap.get("cf1")).isEqualTo(ConsistencyLevel.THREE);
        assertThat(consistencyMap.get("cf2")).isEqualTo(ConsistencyLevel.EACH_QUORUM);
    }

    @Test
    public void should_return_empty_consistency_map_when_no_parameter() throws Exception {
        Map<String, ConsistencyLevel> consistencyMap = extractor.initWriteConsistencyMap(configMap);

        assertThat(consistencyMap).isEmpty();
    }

    @Test
    public void should_return_empty_consistency_map_when_empty_map_parameter() throws Exception {
        configMap.put(CONSISTENCY_LEVEL_WRITE_MAP, new HashMap<String, String>());

        Map<String, ConsistencyLevel> consistencyMap = extractor.initWriteConsistencyMap(configMap);

        assertThat(consistencyMap).isEmpty();
    }

    @Test
    public void should_return_empty_event_interceptor_list_when_empty_list_parameter() throws Exception {
        List<Interceptor<?>> interceptors = extractor.initInterceptors(configMap);
        assertThat(interceptors).isEmpty();
    }

    @Test
    public void should_init_event_interceptor_list() throws Exception {
        ImmutableList<Interceptor<?>> interceptorsExcepted = new ImmutableList.Builder<Interceptor<?>>()
                .add(interceptor1).add(interceptor2).build();
        configMap.put(EVENT_INTERCEPTORS, interceptorsExcepted);

        doCallRealMethod().when(extractor).initInterceptors(configMap);
        List<Interceptor<?>> interceptorsResult = extractor.initInterceptors(configMap);

        assertThat(interceptorsResult).containsExactly(interceptor1, interceptor2);
    }

    @Test
    public void should_init_session() throws Exception {
        ConfigMap params = new ConfigMap();
        params.put(KEYSPACE_NAME, "achilles");

        when(cluster.connect("achilles")).thenReturn(session);

        Session actual = extractor.initSession(cluster, params);

        assertThat(actual).isSameAs(session);
    }

    @Test
    public void should_exception_when_no_keyspace_name_param() throws Exception {
        ConfigMap params = new ConfigMap();

        exception.expect(AchillesException.class);
        exception.expectMessage(KEYSPACE_NAME + " property should be provided");

        extractor.initSession(cluster, params);
    }

    @Test
    public void should_get_native_session_from_parameter() throws Exception {
        ConfigMap params = new ConfigMap();
        params.put(KEYSPACE_NAME, "achilles");
        params.put(NATIVE_SESSION, session);

        Session actual = extractor.initSession(cluster, params);

        assertThat(actual).isSameAs(session);
    }

    @Test
    public void should_init_config_context() throws Exception {
        // Given
        ConfigMap params = new ConfigMap();

        // When
        doReturn(true).when(extractor).initForceTableCreation(params);
        doReturn(factory).when(extractor).initObjectMapperFactory(params);
        doReturn(ANY).when(extractor).initDefaultReadConsistencyLevel(params);
        doReturn(ALL).when(extractor).initDefaultWriteConsistencyLevel(params);

        ConfigurationContext configContext = extractor.initConfigContext(params);

        // Then
        assertThat(configContext.isForceColumnFamilyCreation()).isTrue();
        assertThat(configContext.getObjectMapperFactory()).isSameAs(factory);
        assertThat(configContext.getDefaultReadConsistencyLevel()).isEqualTo(ANY);
        assertThat(configContext.getDefaultWriteConsistencyLevel()).isEqualTo(ALL);
        assertThat(configContext.getDefaultWriteConsistencyLevel()).isEqualTo(ALL);
        assertThat(configContext.getBeanValidator()).isNull();
        assertThat(configContext.getPreparedStatementLRUCacheSize()).isEqualTo(DEFAULT_LRU_CACHE_SIZE);
        assertThat(configContext.isForceBatchStatementsOrdering()).isFalse();
        assertThat(configContext.getInsertStrategy()).isEqualTo(ALL_FIELDS);
        assertThat(configContext.getInsertStrategy()).isEqualTo(ALL_FIELDS);
    }

    @Test
    public void should_return_default_validator() throws Exception {
        // Given
        ConfigMap params = new ConfigMap();
        params.put(BEAN_VALIDATION_ENABLE, true);

        // When
        Validator defaultValidator = extractor.initValidator(params);

        // Then
        assertThat(defaultValidator).isNotNull().isInstanceOf(ValidatorImpl.class);
    }

    @Test
    public void should_return_null_when_bean_validation_disabled() throws Exception {
        // Given
        ConfigMap params = new ConfigMap();
        params.put(BEAN_VALIDATION_ENABLE, false);

        assertThat(extractor.initValidator(params)).isNull();
    }

    @Test
    public void should_return_null_when_bean_validation_not_configured() throws Exception {
        // Given
        ConfigMap params = new ConfigMap();

        assertThat(extractor.initValidator(params)).isNull();
    }

    @Test
    public void should_get_provided_custom_validator() throws Exception {
        // Given
        FakeValidator customValidator = new FakeValidator();
        ConfigMap params = new ConfigMap();
        params.put(BEAN_VALIDATION_ENABLE, true);
        params.put(BEAN_VALIDATION_VALIDATOR, customValidator);

        // When
        Validator validator = extractor.initValidator(params);

        // Then
        assertThat(validator).isNotNull();
        assertThat(validator).isSameAs(customValidator);
    }

    @Test
    public void should_init_max_prepared_statements_cache_size() throws Exception {
        //Given
        ConfigMap params = new ConfigMap();
        params.put(PREPARED_STATEMENTS_CACHE_SIZE, 10);

        //When
        Integer actual = extractor.initPreparedStatementsCacheSize(params);

        //Then
        assertThat(actual).isEqualTo(10);
    }

    @Test
    public void should_init_proxy_warmup() throws Exception {
        //Given
        ConfigMap params = new ConfigMap();
        params.put(PROXIES_WARM_UP_DISABLED, false);

        //When
        boolean actual = extractor.initProxyWarmUp(params);

        //Then
        assertThat(actual).isFalse();
    }

    @Test
    public void should_init_force_batch_statements_ordering() throws Exception {
        //Given
        ConfigMap params = new ConfigMap();
        params.put(FORCE_BATCH_STATEMENTS_ORDERING, true);

        //When
        boolean actual = extractor.initForceBatchStatementsOrdering(params);

        //Then
        assertThat(actual).isTrue();
    }

    @Test
    public void should_init_insert_strategy() throws Exception {
        //Given
        ConfigMap params = new ConfigMap();
        params.put(INSERT_STRATEGY, ALL_FIELDS);

        //When
        final InsertStrategy strategy = extractor.initInsertStrategy(params);

        //Then
        assertThat(strategy).isEqualTo(ALL_FIELDS);
    }

    @Test
    public void should_init_osgi_classloader() throws Exception {
        //Given
        ConfigMap params = new ConfigMap();
        params.put(OSGI_CLASS_LOADER, this.getClass().getClassLoader());

        //When
        final ClassLoader actual = extractor.initOSGIClassLoader(params);

        //Then
        assertThat(actual).isSameAs(this.getClass().getClassLoader());
    }
}
