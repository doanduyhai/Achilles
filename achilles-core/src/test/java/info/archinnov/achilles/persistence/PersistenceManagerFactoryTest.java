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
package info.archinnov.achilles.persistence;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.SchemaContext;
import info.archinnov.achilles.internal.metadata.discovery.AchillesBootstrapper;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.ParsingResult;
import info.archinnov.achilles.type.TypedMap;

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
import com.google.common.collect.ImmutableMap;

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
	private Cluster cluster;

	@Mock
	private Session session;

	@Mock
	private DaoContext daoContext;

	@Mock
	private TypedMap configMap;

	@Captor
	private ArgumentCaptor<SchemaContext> contextCaptor;

	@Before
	public void setUp() {
		pmf = new PersistenceManagerFactory(ImmutableMap.<String, Object> of("test", "test"));
		pmf.configurationMap = configMap;
		Whitebox.setInternalState(pmf, ArgumentExtractor.class, argumentExtractor);
		Whitebox.setInternalState(pmf, AchillesBootstrapper.class, boostrapper);
	}

	@Test
	public void should_bootstrap_persistence_manager_factory() throws Exception {
		// Given
		List<String> entityPackages = Arrays.asList();
		List<Class<?>> candidateClasses = Arrays.asList();
		List<Interceptor<?>> interceptors = Arrays.asList();
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();
		ParsingResult parsingResult = new ParsingResult(entityMetaMap, true);

		// When
		when(argumentExtractor.initEntityPackages(configMap)).thenReturn(entityPackages);
		when(argumentExtractor.initConfigContext(configMap)).thenReturn(configContext);
		when(argumentExtractor.initCluster(configMap)).thenReturn(cluster);
		when(argumentExtractor.initSession(cluster, configMap)).thenReturn(session);
		when(argumentExtractor.initInterceptors(configMap)).thenReturn(interceptors);

		when(boostrapper.discoverEntities(entityPackages)).thenReturn(candidateClasses);
		when(configMap.getTyped(ENTITY_PACKAGES_PARAM)).thenReturn("packages");
		when(configMap.getTyped(KEYSPACE_NAME_PARAM)).thenReturn("keyspace");
		when(boostrapper.buildMetaDatas(configContext, candidateClasses)).thenReturn(parsingResult);
		when(configContext.isForceColumnFamilyCreation()).thenReturn(true);
		when(boostrapper.buildDaoContext(session, entityMetaMap, true)).thenReturn(daoContext);

		pmf.bootstrap();

		// Then
		assertThat(pmf.entityMetaMap).isSameAs(entityMetaMap);
		assertThat(pmf.configContext).isSameAs(configContext);
		assertThat(pmf.daoContext).isSameAs(daoContext);
		PersistenceContextFactory contextFactory = Whitebox.getInternalState(pmf, PersistenceContextFactory.class);
		assertThat(Whitebox.getInternalState(contextFactory, DaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(contextFactory, ConfigurationContext.class)).isSameAs(configContext);
		assertThat(Whitebox.getInternalState(contextFactory, "entityMetaMap")).isSameAs(entityMetaMap);

		verify(boostrapper).validateOrCreateTables(contextCaptor.capture());
		verify(boostrapper).addInterceptorsToEntityMetas(interceptors, entityMetaMap);

		SchemaContext schemaContext = contextCaptor.getValue();

		assertThat(Whitebox.getInternalState(schemaContext, Cluster.class)).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(schemaContext, Session.class)).isSameAs(session);
		assertThat(Whitebox.getInternalState(schemaContext, "entityMetaMap")).isSameAs(entityMetaMap);
		assertThat(Whitebox.getInternalState(schemaContext, "keyspaceName")).isEqualTo("keyspace");
		assertThat((Boolean) Whitebox.getInternalState(schemaContext, "forceColumnFamilyCreation")).isTrue();
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

		PersistenceManager manager = pmf.createBatchingPersistenceManager();

		// Then
		assertThat(manager).isNotNull();
	}
}
