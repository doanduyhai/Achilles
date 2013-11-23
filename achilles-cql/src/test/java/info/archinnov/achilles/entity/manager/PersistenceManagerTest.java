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
package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.query.cql.CQLNativeQueryBuilder;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQueryBuilder;
import info.archinnov.achilles.query.typed.TypedQueryValidator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
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

import com.datastax.driver.core.Session;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private EntityInitializer initializer;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private EntityValidator entityValidator;

	@Mock
	private TypedQueryValidator typedQueryValidator;

	@Mock
	private SliceQueryExecutor sliceQueryExecutor;

	@Mock
	private PersistenceManagerFactory pmf;

	@Mock
	private PersistenceContextFactory contextFactory;

	@Mock
	private DaoContext daoContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private PersistenceContext context;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

	@Mock
	private EntityMeta meta;

	@Mock
	private PropertyMeta idMeta;

	@Captor
	private ArgumentCaptor<Options> optionsCaptor;

	private PersistenceManager manager;

	private Long primaryKey = RandomUtils.nextLong();
	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	@Before
	public void setUp() throws Exception {
		when(contextFactory.newContext(eq(entity), optionsCaptor.capture())).thenReturn(context);
		when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);
		when(meta.getIdMeta()).thenReturn(idMeta);

		manager = new PersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
		Whitebox.setInternalState(manager, EntityProxifier.class, proxifier);
		Whitebox.setInternalState(manager, EntityValidator.class, entityValidator);
		Whitebox.setInternalState(manager, SliceQueryExecutor.class, sliceQueryExecutor);
		Whitebox.setInternalState(manager, TypedQueryValidator.class, typedQueryValidator);
		Whitebox.setInternalState(manager, PersistenceContextFactory.class, contextFactory);

		manager.setEntityMetaMap(entityMetaMap);
		entityMetaMap.put(CompleteBean.class, meta);
	}

	@Test
	public void should_persist() throws Exception {
		// When
		when(proxifier.isProxy(entity)).thenReturn(false);

		manager.persist(entity);

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(context).persist();
	}

	@Test
	public void should_persist_with_options() throws Exception {
		// When
		when(proxifier.isProxy(entity)).thenReturn(false);
		manager.persist(entity, OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(150).withTimestamp(100L));

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(context).persist();

		Options value = optionsCaptor.getValue();
		assertThat(value.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
		assertThat(value.getTtl().get()).isEqualTo(150);
		assertThat(value.getTimestamp().get()).isEqualTo(100L);
	}

	@Test
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception {
		// When
		when(proxifier.isProxy(entity)).thenReturn(true);

		exception.expect(IllegalStateException.class);
		exception
				.expectMessage("Then entity is already in 'managed' state. Please use the merge() method instead of persist()");

		manager.persist(entity);
	}

	@Test
	public void should_merge() throws Exception {
		// When
		when(context.merge(entity)).thenReturn(entity);
		CompleteBean mergedEntity = manager.merge(entity);

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);

		assertThat(mergedEntity).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_merge_with_options() throws Exception {
		// When
		when(context.merge(entity)).thenReturn(entity);
		CompleteBean mergedEntity = manager.merge(entity, OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(150)
				.withTimestamp(100L));

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		assertThat(mergedEntity).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
		assertThat(options.getTtl().get()).isEqualTo(150);
		assertThat(options.getTimestamp().get()).isEqualTo(100L);
	}

	@Test
	public void should_remove() throws Exception {
		// When
        when(proxifier.getRealObject(entity)).thenReturn(entity);

		manager.remove(entity);

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_remove_with_consistency() throws Exception {
		// When
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        manager.remove(entity, EACH_QUORUM);

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_remove_by_id() throws Exception {
		// When
		when(contextFactory.newContext(CompleteBean.class, primaryKey, OptionsBuilder.noOptions())).thenReturn(context);
		when(context.getIdMeta()).thenReturn(idMeta);

		manager.removeById(CompleteBean.class, primaryKey);

		// Then
		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		verify(context).remove();
	}

	@Test
	public void should_remove_by_id_with_consistency() throws Exception {
		// When
		when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(
                context);

		when(context.getIdMeta()).thenReturn(idMeta);

		manager.removeById(CompleteBean.class, primaryKey, LOCAL_QUORUM);

		// Then
		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		verify(context).remove();

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(LOCAL_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_find() throws Exception {
		// When
		when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(
                context);
		when(context.find(CompleteBean.class)).thenReturn(entity);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.find(CompleteBean.class, primaryKey);

		// Then
		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_find_with_consistency() throws Exception {
		// When
		when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(
				context);
		when(context.find(CompleteBean.class)).thenReturn(entity);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.find(CompleteBean.class, primaryKey, EACH_QUORUM);

		// Then
		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_get_reference() throws Exception {
		// When
		when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(
				context);
		when(context.getReference(CompleteBean.class)).thenReturn(entity);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.getReference(CompleteBean.class, primaryKey);

		// Then
		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_get_reference_with_consistency() throws Exception {
		// When
		when(contextFactory.newContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture())).thenReturn(
				context);
		when(context.getReference(CompleteBean.class)).thenReturn(entity);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.getReference(CompleteBean.class, primaryKey, EACH_QUORUM);

		// Then
		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_refresh() throws Exception {
		// When
		manager.refresh(entity);

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(context).refresh();

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_refresh_with_consistency() throws Exception {
		// When
		manager.refresh(entity, EACH_QUORUM);

		// Then
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(context).refresh();

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_initialize_entity() throws Exception {
		when(context.initialize(entity)).thenReturn(entity);
		CompleteBean actual = manager.initialize(entity);
		verify(proxifier).ensureProxy(entity);
		assertThat(actual).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_initialize_list_of_entities() throws Exception {
		// Given
		List<CompleteBean> entities = Arrays.asList(entity);

		// When
		when(context.initialize(entity)).thenReturn(entity);
		List<CompleteBean> actual = manager.initialize(entities);

		// Then
		assertThat(actual).containsExactly(entity);
	}

	@Test
	public void should_initialize_set_of_entities() throws Exception {
		// Given
		Set<CompleteBean> entities = Sets.newHashSet(entity);

		// When
		when(context.initialize(entity)).thenReturn(entity);
		Set<CompleteBean> actual = manager.initialize(entities);

		// Then
		assertThat(actual).containsExactly(entity);
	}

	@Test
	public void should_unwrap_entity() throws Exception {
		// When
		when(proxifier.unwrap(entity)).thenReturn(entity);
		CompleteBean actual = manager.unwrap(entity);

		// Then
		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_unwrap_list_of_entity() throws Exception {
		// Given
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();

		// When
		when(proxifier.unwrap(proxies)).thenReturn(proxies);

		List<CompleteBean> actual = manager.unwrap(proxies);

		// Then
		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unwrap_set_of_entity() throws Exception {
		// Given
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		// When
		when(proxifier.unwrap(proxies)).thenReturn(proxies);

		Set<CompleteBean> actual = manager.unwrap(proxies);

		// Then
		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_init_and_unwrap_entity() throws Exception {
		// When
		when(context.initialize(entity)).thenReturn(entity);
		when(proxifier.unwrap(entity)).thenReturn(entity);

		CompleteBean actual = manager.initAndUnwrap(entity);

		// Then
		assertThat(actual).isSameAs(entity);

	}

	@Test
	public void should_init_and_unwrap_list_of_entities() throws Exception {
		// Given
		List<CompleteBean> entities = Arrays.asList(entity);

		// When
		when(context.initialize(entities)).thenReturn(entities);
		when(proxifier.unwrap(entities)).thenReturn(entities);

		List<CompleteBean> actual = manager.initAndUnwrap(entities);

		// Then
		assertThat(actual).isSameAs(entities);
	}

	@Test
	public void should_init_and_unwrap_set_of_entities() throws Exception {
		// Given
		Set<CompleteBean> entities = Sets.newHashSet(entity);

		// When
		when(context.initialize(entities)).thenReturn(entities);
		when(proxifier.unwrap(entities)).thenReturn(entities);

		Set<CompleteBean> actual = manager.initAndUnwrap(entities);

		// Then
		assertThat(actual).isSameAs(entities);
	}

	@Test
	public void should_return_slice_query_builder() throws Exception {
		// When
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
		when(meta.isClusteredEntity()).thenReturn(true);

		SliceQueryBuilder<CompleteBean> builder = manager.sliceQuery(CompleteBean.class);

		// Then
		assertThat(Whitebox.getInternalState(builder, SliceQueryExecutor.class)).isSameAs(sliceQueryExecutor);
		assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
		assertThat(Whitebox.getInternalState(builder, PropertyMeta.class)).isSameAs(idMeta);
	}

	@Test
	public void should_return_native_query_builder() throws Exception {
		// When
		CQLNativeQueryBuilder builder = manager.nativeQuery("queryString");

		assertThat(builder).isNotNull();

		// Then
		assertThat(Whitebox.getInternalState(builder, DaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("queryString");
	}

	@Test
	public void should_return_typed_query_builder() throws Exception {
		// When
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
		when(meta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta>());

		TypedQueryBuilder<CompleteBean> builder = manager.typedQuery(CompleteBean.class, "queryString");

		// Then
		assertThat(builder).isNotNull();

		verify(typedQueryValidator).validateTypedQuery(CompleteBean.class, "queryString", meta);

		assertThat(Whitebox.getInternalState(builder, DaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(builder, Class.class)).isEqualTo(CompleteBean.class);
		assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
		assertThat(Whitebox.getInternalState(builder, PersistenceContextFactory.class)).isSameAs(contextFactory);
		assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
	}

	@Test
	public void should_return_raw_typed_query_builder() throws Exception {
		// When
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
		when(meta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta>());

		TypedQueryBuilder<CompleteBean> builder = manager.rawTypedQuery(CompleteBean.class, "queryString");

		// Then
		assertThat(builder).isNotNull();

		verify(typedQueryValidator).validateRawTypedQuery(CompleteBean.class, "queryString", meta);

		assertThat(Whitebox.getInternalState(builder, DaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(builder, Class.class)).isEqualTo(CompleteBean.class);
		assertThat(Whitebox.getInternalState(builder, EntityMeta.class)).isSameAs(meta);
		assertThat(Whitebox.getInternalState(builder, PersistenceContextFactory.class)).isSameAs(contextFactory);
		assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
	}

	@Test
	public void should_get_native_session() throws Exception {
		// Given
		Session session = mock(Session.class);

		// When
		when(daoContext.getSession()).thenReturn(session);

		Session actual = manager.getNativeSession();

		// Then
		assertThat(actual).isSameAs(session);
	}

	@Test
	public void should_get_indexed_query() throws Exception {
		// When
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(meta);
		when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);
		when(meta.isClusteredEntity()).thenReturn(false);
		when(meta.getTableName()).thenReturn("table");

		manager.indexedQuery(CompleteBean.class, new IndexCondition("column", "value"));

		// Then
		verify(typedQueryValidator).validateTypedQuery(CompleteBean.class, "SELECT * FROM table WHERE column='value';",
                                                       meta);
	}
}
