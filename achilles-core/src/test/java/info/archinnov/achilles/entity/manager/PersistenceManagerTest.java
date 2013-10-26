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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private PersistenceManager<PersistenceContext> manager;

	@Mock
	private EntityPersister<PersistenceContext> persister;

	@Mock
	private EntityLoader<PersistenceContext> loader;

	@Mock
	private EntityMerger<PersistenceContext> merger;

	@Mock
	private EntityRefresher<PersistenceContext> refresher;

	@Mock
	private EntityInitializer initializer;

	@Mock
	private EntityProxifier<PersistenceContext> proxifier;

	@Mock
	private EntityValidator<PersistenceContext> entityValidator;

	@Mock
	private PersistenceContext context;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

	@Mock
	private EntityMeta entityMeta;

	@Captor
	private ArgumentCaptor<Options> optionsCaptor;

	private Long primaryKey = 1165446L;
	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).name("name").buid();

	@Before
	public void setUp() throws Exception {

		forceMethodCallsOnMock();
		when(manager.initPersistenceContext(eq(CompleteBean.class), eq(primaryKey), optionsCaptor.capture()))
				.thenReturn(context);
		when(manager.initPersistenceContext(eq(entity), optionsCaptor.capture())).thenReturn(context);
	}

	@Test
	public void should_persist() throws Exception {
		when(proxifier.isProxy(entity)).thenReturn(false);
		doCallRealMethod().when(manager).persist(entity);
		doCallRealMethod().when(manager).persist(eq(entity), any(Options.class));

		manager.persist(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(context).persist();

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_persist_with_options() throws Exception {
		when(proxifier.isProxy(entity)).thenReturn(false);
		doCallRealMethod().when(manager).persist(eq(entity), optionsCaptor.capture());

		manager.persist(entity, OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(150).withTimestamp(100L));

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(context).persist();

		Options value = optionsCaptor.getValue();
		assertThat(value.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
		assertThat(value.getTtl().get()).isEqualTo(150);
		assertThat(value.getTimestamp().get()).isEqualTo(100L);
	}

	@Test
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception {
		when(proxifier.isProxy(entity)).thenReturn(true);
		doCallRealMethod().when(manager).persist(entity);
		doCallRealMethod().when(manager).persist(eq(entity), any(Options.class));

		exception.expect(IllegalStateException.class);

		manager.persist(entity);
	}

	@Test
	public void should_merge() throws Exception {
		when(context.merge(entity)).thenReturn(entity);
		doCallRealMethod().when(manager).merge(entity);
		doCallRealMethod().when(manager).merge(eq(entity), optionsCaptor.capture());

		CompleteBean mergedEntity = manager.merge(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);

		assertThat(mergedEntity).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_merge_with_options() throws Exception {
		when(context.merge(entity)).thenReturn(entity);
		doCallRealMethod().when(manager).merge(eq(entity), optionsCaptor.capture());

		CompleteBean mergedEntity = manager.merge(entity, OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(150)
				.withTimestamp(100L));

		verify(entityValidator).validateEntity(entity, entityMetaMap);

		assertThat(mergedEntity).isSameAs(entity);
		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
		assertThat(options.getTtl().get()).isEqualTo(150);
		assertThat(options.getTimestamp().get()).isEqualTo(100L);
	}

	@Test
	public void should_remove() throws Exception {
		doCallRealMethod().when(manager).remove(entity);
		doCallRealMethod().when(manager).remove(eq(entity), any(ConsistencyLevel.class));

		manager.remove(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_remove_with_consistency() throws Exception {
		when(manager.initPersistenceContext(eq(entity), optionsCaptor.capture())).thenReturn(context);
		doCallRealMethod().when(manager).remove(entity, EACH_QUORUM);
		doCallRealMethod().when(manager).remove(eq(entity), any(ConsistencyLevel.class));

		manager.remove(entity, EACH_QUORUM);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_remove_by_id() throws Exception {
		doCallRealMethod().when(manager).removeById(CompleteBean.class, primaryKey);
		PropertyMeta idMeta = new PropertyMeta();
		when(context.getIdMeta()).thenReturn(idMeta);

		manager.removeById(CompleteBean.class, primaryKey);

		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		verify(context).remove();

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_remove_by_id_with_consistency() throws Exception {
		doCallRealMethod().when(manager).removeById(CompleteBean.class, primaryKey, LOCAL_QUORUM);
		PropertyMeta idMeta = new PropertyMeta();
		when(context.getIdMeta()).thenReturn(idMeta);

		manager.removeById(CompleteBean.class, primaryKey, LOCAL_QUORUM);

		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		verify(context).remove();
		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(LOCAL_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_find() throws Exception {
		doCallRealMethod().when(manager).find(CompleteBean.class, primaryKey);
		doCallRealMethod().when(manager).find(eq(CompleteBean.class), eq(primaryKey), any(ConsistencyLevel.class));

		when(context.find(CompleteBean.class)).thenReturn(entity);
		PropertyMeta idMeta = new PropertyMeta();
		when(context.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.find(CompleteBean.class, primaryKey);


        verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_find_with_consistency() throws Exception {
		doCallRealMethod().when(manager).find(CompleteBean.class, primaryKey, EACH_QUORUM);
		doCallRealMethod().when(manager).find(eq(CompleteBean.class), eq(primaryKey), any(ConsistencyLevel.class));

		when(context.find(CompleteBean.class)).thenReturn(entity);
		PropertyMeta idMeta = new PropertyMeta();
		when(context.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.find(CompleteBean.class, primaryKey, EACH_QUORUM);

		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_get_reference() throws Exception {
		when(context.getReference(CompleteBean.class)).thenReturn(entity);
		doCallRealMethod().when(manager).getReference(CompleteBean.class, primaryKey);
		doCallRealMethod().when(manager).getReference(eq(CompleteBean.class), eq(primaryKey),
				any(ConsistencyLevel.class));
		PropertyMeta idMeta = new PropertyMeta();
		when(context.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);


		CompleteBean bean = manager.getReference(CompleteBean.class, primaryKey);

		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_get_reference_with_consistency() throws Exception {
		when(context.getReference(CompleteBean.class)).thenReturn(entity);
		doCallRealMethod().when(manager).getReference(CompleteBean.class, primaryKey, EACH_QUORUM);
		doCallRealMethod().when(manager).getReference(eq(CompleteBean.class), eq(primaryKey),
				any(ConsistencyLevel.class));
		PropertyMeta idMeta = new PropertyMeta();
		when(context.getIdMeta()).thenReturn(idMeta);
        when(entityMetaMap.containsKey(CompleteBean.class)).thenReturn(true);

		CompleteBean bean = manager.getReference(CompleteBean.class, primaryKey, EACH_QUORUM);

		verify(entityValidator).validatePrimaryKey(idMeta, primaryKey);
		assertThat(bean).isSameAs(entity);

		Options options = optionsCaptor.getValue();
		assertThat(options.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
	}

	@Test
	public void should_refresh() throws Exception {
		doCallRealMethod().when(manager).refresh(entity);
		doCallRealMethod().when(manager).refresh(eq(entity), any(ConsistencyLevel.class));

		manager.refresh(entity);

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
		doCallRealMethod().when(manager).refresh(entity, EACH_QUORUM);
		doCallRealMethod().when(manager).refresh(eq(entity), any(ConsistencyLevel.class));

		manager.refresh(entity, EACH_QUORUM);

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
		doCallRealMethod().when(manager).initialize(entity);
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
		when(manager.initialize(entity)).thenReturn(entity);
		List<CompleteBean> entities = Arrays.asList(entity);
		doCallRealMethod().when(manager).initialize(entities);
		List<CompleteBean> actual = manager.initialize(entities);

		assertThat(actual).containsExactly(entity);

	}

	@Test
	public void should_initialize_set_of_entities() throws Exception {
		when(manager.initialize(entity)).thenReturn(entity);
		Set<CompleteBean> entities = Sets.newHashSet(entity);
		doCallRealMethod().when(manager).initialize(entities);
		Set<CompleteBean> actual = manager.initialize(entities);

		assertThat(actual).containsExactly(entity);

	}

	@Test
	public void should_unwrap_entity() throws Exception {
		when(proxifier.unwrap(entity)).thenReturn(entity);
		doCallRealMethod().when(manager).unwrap(entity);
		CompleteBean actual = manager.unwrap(entity);

		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_unwrap_list_of_entity() throws Exception {
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();
		when(proxifier.unwrap(proxies)).thenReturn(proxies);

		doCallRealMethod().when(manager).unwrap(proxies);
		List<CompleteBean> actual = manager.unwrap(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unwrap_set_of_entity() throws Exception {
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		when(proxifier.unwrap(proxies)).thenReturn(proxies);

		doCallRealMethod().when(manager).unwrap(proxies);
		Set<CompleteBean> actual = manager.unwrap(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_init_and_unwrap_entity() throws Exception {
		when(manager.initialize(entity)).thenReturn(entity);
		when(manager.unwrap(entity)).thenReturn(entity);

		doCallRealMethod().when(manager).initAndUnwrap(entity);

		CompleteBean actual = manager.initAndUnwrap(entity);

		assertThat(actual).isSameAs(entity);

	}

	@Test
	public void should_init_and_unwrap_list_of_entities() throws Exception {
		List<CompleteBean> entities = Arrays.asList(entity);

		when(manager.initialize(entities)).thenReturn(entities);
		when(manager.unwrap(entities)).thenReturn(entities);

		doCallRealMethod().when(manager).initAndUnwrap(entities);

		List<CompleteBean> actual = manager.initAndUnwrap(entities);

		assertThat(actual).isSameAs(entities);
	}

	@Test
	public void should_init_and_unwrap_set_of_entities() throws Exception {
		Set<CompleteBean> entities = Sets.newHashSet(entity);

		when(manager.initialize(entities)).thenReturn(entities);
		when(manager.unwrap(entities)).thenReturn(entities);

		doCallRealMethod().when(manager).initAndUnwrap(entities);

		Set<CompleteBean> actual = manager.initAndUnwrap(entities);

		assertThat(actual).isSameAs(entities);
	}

	private void forceMethodCallsOnMock() {
		doCallRealMethod().when(manager).setInitializer(initializer);
		manager.setInitializer(initializer);

		doCallRealMethod().when(manager).setProxifier(proxifier);
		manager.setProxifier(proxifier);

		doCallRealMethod().when(manager).setEntityValidator(entityValidator);
		manager.setEntityValidator(entityValidator);

		doCallRealMethod().when(manager).setEntityMetaMap(entityMetaMap);
		manager.setEntityMetaMap(entityMetaMap);
	}
}
