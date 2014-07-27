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
package info.archinnov.achilles.persistence;

import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.SchemaContext;
import info.archinnov.achilles.internal.metadata.discovery.AchillesBootstrapper;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.ParsingResult;
import info.archinnov.achilles.internal.proxy.ProxyClassFactory;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerFactoryTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PersistenceManagerFactory pmf;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private ArgumentExtractor argumentExtractor;

    @Mock
    private AchillesBootstrapper boostrapper;

    @Mock
    private ProxyClassFactory proxyClassFactory;

    @Mock
    private Cluster cluster;

    @Mock
    private Session session;

    @Mock
    private DaoContext daoContext;

    @Mock
    private ConfigMap configMap;

    @Captor
    private ArgumentCaptor<SchemaContext> contextCaptor;

    @Before
    public void setUp() {
        pmf = new PersistenceManagerFactory(cluster, ImmutableMap.<ConfigurationParameters, Object>of(FORCE_TABLE_CREATION, true));
        pmf.configurationMap = configMap;
        Whitebox.setInternalState(pmf, ArgumentExtractor.class, argumentExtractor);
        Whitebox.setInternalState(pmf, AchillesBootstrapper.class, boostrapper);
        Whitebox.setInternalState(pmf, ProxyClassFactory.class, proxyClassFactory);
    }

    @Test
    public void should_bootstrap_persistence_manager_factory() throws Exception {
        // Given
        List<Class<?>> candidateClasses = Arrays.asList();
        List<Interceptor<?>> interceptors = Arrays.asList();
        Map<Class<?>, EntityMeta> entityMetaMap = ImmutableMap.<Class<?>, EntityMeta>of(CompleteBean.class,
                new EntityMeta());
        ParsingResult parsingResult = new ParsingResult(entityMetaMap, true);
        final ClassLoader classLoader = this.getClass().getClassLoader();

        // When
        when(argumentExtractor.initConfigContext(configMap)).thenReturn(configContext);
        when(argumentExtractor.initSession(cluster, configMap)).thenReturn(session);
        when(argumentExtractor.initInterceptors(configMap)).thenReturn(interceptors);
        when(argumentExtractor.initProxyWarmUp(configMap)).thenReturn(true);
        when(argumentExtractor.initOSGIClassLoader(configMap)).thenReturn(classLoader);
        when(argumentExtractor.initEntities(configMap, classLoader)).thenReturn(candidateClasses);

        when(configMap.getTyped(ENTITY_PACKAGES)).thenReturn("packages");
        when(configMap.getTyped(KEYSPACE_NAME)).thenReturn("keyspace");
        when(boostrapper.buildMetaDatas(configContext, candidateClasses)).thenReturn(parsingResult);
        when(configContext.isForceColumnFamilyCreation()).thenReturn(true);
        when(boostrapper.buildDaoContext(session, parsingResult, configContext)).thenReturn(daoContext);

        pmf.bootstrap();

        // Then
        assertThat(pmf.entityMetaMap).isSameAs(entityMetaMap);
        assertThat(pmf.configContext).isSameAs(configContext);
        assertThat(pmf.daoContext).isSameAs(daoContext);
        PersistenceContextFactory contextFactory = Whitebox.getInternalState(pmf, PersistenceContextFactory.class);
        assertThat(Whitebox.getInternalState(contextFactory, DaoContext.class)).isSameAs(daoContext);
        assertThat(Whitebox.getInternalState(contextFactory, ConfigurationContext.class)).isSameAs(configContext);
        assertThat(Whitebox.getInternalState(contextFactory, "entityMetaMap")).isSameAs(entityMetaMap);

        verify(boostrapper).buildMetaDatas(configContext, candidateClasses);
        verify(boostrapper).validateOrCreateTables(contextCaptor.capture());
        verify(boostrapper).addInterceptorsToEntityMetas(interceptors, entityMetaMap);
        verify(proxyClassFactory).createProxyClass(CompleteBean.class, configContext);

        SchemaContext schemaContext = contextCaptor.getValue();

        assertThat(Whitebox.getInternalState(schemaContext, Cluster.class)).isSameAs(cluster);
        assertThat(Whitebox.getInternalState(schemaContext, Session.class)).isSameAs(session);
        assertThat(Whitebox.getInternalState(schemaContext, "entityMetaMap")).isSameAs(entityMetaMap);
        assertThat(Whitebox.getInternalState(schemaContext, "keyspaceName")).isEqualTo("keyspace");
        assertThat((ConfigurationContext) Whitebox.getInternalState(schemaContext, "configContext")).isSameAs(configContext);
        assertThat((Boolean) Whitebox.getInternalState(schemaContext, "hasCounter")).isTrue();
    }

    @Test
    public void should_create_persistence_manager() throws Exception {
        // Given
        Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
        PersistenceContextFactory contextFactory = mock(PersistenceContextFactory.class);

        // When
        pmf.entityMetaMap = entityMetaMap;
        pmf.configContext = configContext;
        pmf.daoContext = daoContext;
        pmf.contextFactory = contextFactory;

        PersistenceManager manager = pmf.createPersistenceManager();

        // Then
        assertThat(manager).isNotNull();
    }

    @Test
    public void should_create_batching_persistence_manager() throws Exception {
        // Given
        Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
        PersistenceContextFactory contextFactory = mock(PersistenceContextFactory.class);

        // When
        pmf.entityMetaMap = entityMetaMap;
        pmf.configContext = configContext;
        pmf.daoContext = daoContext;
        pmf.contextFactory = contextFactory;

        Batch batch = pmf.createBatch();

        // Then
        assertThat(batch).isNotNull();
    }

    @Test
    public void should_serialize_to_json() throws Exception {
        //Given
        pmf.configContext = configContext;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        when(configContext.getMapperFor(CompleteBean.class)).thenReturn(mapper);
        CompleteBean entity = CompleteBeanTestBuilder.builder().id(10L).name("name").buid();

        //When
        final String serialized = pmf.serializeToJSON(entity);

        //Then
        assertThat(serialized).isEqualTo("{\"id\":10,\"name\":\"name\",\"friends\":[],\"followers\":[],\"preferences\":{}}");
    }

    @Test
    public void should_deserialize_from_json() throws Exception {
        //Given
        pmf.configContext = configContext;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        when(configContext.getMapperFor(CompleteBean.class)).thenReturn(mapper);

        //When
        final CompleteBean actual = pmf.deserializeFromJSON(CompleteBean.class, "{\"id\":10,\"name\":\"name\"}");

        //Then
        assertThat(actual.getId()).isEqualTo(10L);
        assertThat(actual.getName()).isEqualTo("name");
        assertThat(actual.getFriends()).isNull();
        assertThat(actual.getFollowers()).isNull();
        assertThat(actual.getPreferences()).isNull();
    }
}
