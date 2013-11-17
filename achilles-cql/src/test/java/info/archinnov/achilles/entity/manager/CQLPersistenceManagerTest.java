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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityInitializer;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.CQLEntityRefresher;
import info.archinnov.achilles.entity.operations.CQLEntityValidator;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.query.cql.CQLNativeQueryBuilder;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryBuilder;
import info.archinnov.achilles.query.typed.CQLTypedQueryValidator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
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
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceManagerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock
    private CQLEntityPersister persister;

    @Mock
    private CQLEntityLoader loader;

    @Mock
    private CQLEntityMerger merger;

    @Mock
    private CQLEntityRefresher refresher;

    @Mock
    private CQLEntityInitializer initializer;

    @Mock
    private CQLEntityProxifier proxifier;

    @Mock
    private CQLEntityValidator entityValidator;

	@Mock
	private CQLPersistenceManagerFactory pmf;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private CQLPersistenceContextFactory contextFactory;

    @Mock
    private CQLPersistenceContext context;

	@Mock
	private CQLTypedQueryValidator typedQueryValidator;

    @Captor
    private ArgumentCaptor<Options> optionsCaptor;

    private CQLPersistenceManager manager;

    private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

	private EntityMeta meta;

	private PropertyMeta idMeta;

    private Long primaryKey = RandomUtils.nextLong();
    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	@Before
	public void setUp() throws Exception {
		idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors()
				.type(PropertyType.SIMPLE).build();

		meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setTableName("table");
		meta.setEntityClass(CompleteBean.class);
		meta.setPropertyMetas(new HashMap<String, PropertyMeta>());

		when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);

		manager = new CQLPersistenceManager(entityMetaMap, contextFactory, daoContext, configContext);
		Whitebox.setInternalState(manager, CQLEntityProxifier.class, proxifier);
		Whitebox.setInternalState(manager, CQLTypedQueryValidator.class, typedQueryValidator);

		manager.setEntityMetaMap(entityMetaMap);
		entityMetaMap.put(CompleteBean.class, meta);
	}

    @Test
    public void should_persist() throws Exception {
        when(proxifier.isProxy(entity)).thenReturn(false);

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

        exception.expect(IllegalStateException.class);
        exception.expectMessage("toto");

        manager.persist(entity);
    }

    @Test
    public void should_merge() throws Exception {
        when(context.merge(entity)).thenReturn(entity);

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
        List<CompleteBean> actual = manager.initialize(entities);

        assertThat(actual).containsExactly(entity);

    }

    @Test
    public void should_initialize_set_of_entities() throws Exception {
        when(manager.initialize(entity)).thenReturn(entity);
        Set<CompleteBean> entities = Sets.newHashSet(entity);
        Set<CompleteBean> actual = manager.initialize(entities);

        assertThat(actual).containsExactly(entity);

    }

    @Test
    public void should_unwrap_entity() throws Exception {
        when(proxifier.unwrap(entity)).thenReturn(entity);
        CompleteBean actual = manager.unwrap(entity);

        assertThat(actual).isSameAs(entity);
    }

    @Test
    public void should_unwrap_list_of_entity() throws Exception {
        List<CompleteBean> proxies = new ArrayList<CompleteBean>();
        when(proxifier.unwrap(proxies)).thenReturn(proxies);

        List<CompleteBean> actual = manager.unwrap(proxies);

        assertThat(actual).isSameAs(proxies);
    }

    @Test
    public void should_unwrap_set_of_entity() throws Exception {
        Set<CompleteBean> proxies = new HashSet<CompleteBean>();

        when(proxifier.unwrap(proxies)).thenReturn(proxies);

        Set<CompleteBean> actual = manager.unwrap(proxies);

        assertThat(actual).isSameAs(proxies);
    }

    @Test
    public void should_init_and_unwrap_entity() throws Exception {
        when(manager.initialize(entity)).thenReturn(entity);
        when(manager.unwrap(entity)).thenReturn(entity);

        CompleteBean actual = manager.initAndUnwrap(entity);

        assertThat(actual).isSameAs(entity);

    }

    @Test
    public void should_init_and_unwrap_list_of_entities() throws Exception {
        List<CompleteBean> entities = Arrays.asList(entity);

        when(manager.initialize(entities)).thenReturn(entities);
        when(manager.unwrap(entities)).thenReturn(entities);

        List<CompleteBean> actual = manager.initAndUnwrap(entities);

        assertThat(actual).isSameAs(entities);
    }

    @Test
    public void should_init_and_unwrap_set_of_entities() throws Exception {
        Set<CompleteBean> entities = Sets.newHashSet(entity);

        when(manager.initialize(entities)).thenReturn(entities);
        when(manager.unwrap(entities)).thenReturn(entities);

        Set<CompleteBean> actual = manager.initAndUnwrap(entities);

        assertThat(actual).isSameAs(entities);
    }
	@Test
	public void should_init_persistence_context_with_entity() throws Exception {
		CQLPersistenceContext context = mock(CQLPersistenceContext.class);
		when(contextFactory.newContext(entity, OptionsBuilder.noOptions())).thenReturn(context);

		CQLPersistenceContext actual = manager.initPersistenceContext(entity, OptionsBuilder.noOptions());

		assertThat(actual).isSameAs(context);

	}

	@Test
	public void should_init_persistence_context_with_type_and_id() throws Exception {
		CQLPersistenceContext context = mock(CQLPersistenceContext.class);
		when(contextFactory.newContext(CompleteBean.class, entity.getId(), OptionsBuilder.noOptions())).thenReturn(
				context);

		CQLPersistenceContext actual = manager.initPersistenceContext(CompleteBean.class, entity.getId(),
				OptionsBuilder.noOptions());

		assertThat(actual).isSameAs(context);
	}

	@Test(expected=AchillesException.class)
	public void should_return_slice_query_builder() throws Exception {
		SliceQueryBuilder<CompleteBean> builder = manager.sliceQuery(CompleteBean.class);
	}

	@Test
	public void should_return_native_query_builder() throws Exception {
		CQLNativeQueryBuilder builder = manager.nativeQuery("queryString");

		assertThat(builder).isNotNull();

		assertThat(Whitebox.getInternalState(builder, CQLDaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("queryString");
	}

	@Test
	public void should_return_typed_query_builder() throws Exception {

		CQLTypedQueryBuilder<CompleteBean> builder = manager.typedQuery(CompleteBean.class, "queryString");

		assertThat(builder).isNotNull();

		verify(typedQueryValidator).validateTypedQuery(CompleteBean.class, "queryString", meta);

		assertThat(Whitebox.getInternalState(builder, CQLDaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
		assertThat(Whitebox.getInternalState(builder, Class.class)).isEqualTo(CompleteBean.class);
	}

	@Test
	public void should_return_raw_typed_query_builder() throws Exception {

		CQLTypedQueryBuilder<CompleteBean> builder = manager.rawTypedQuery(CompleteBean.class, "queryString");

		assertThat(builder).isNotNull();

		verify(typedQueryValidator).validateRawTypedQuery(CompleteBean.class, "queryString", meta);

		assertThat(Whitebox.getInternalState(builder, CQLDaoContext.class)).isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(builder, String.class)).isEqualTo("querystring");
		assertThat(Whitebox.getInternalState(builder, Class.class)).isEqualTo(CompleteBean.class);
	}

	@Test
	public void should_get_native_session() throws Exception {
		Session session = mock(Session.class);
		when(daoContext.getSession()).thenReturn(session);

		Session actual = manager.getNativeSession();

		assertThat(actual).isSameAs(session);
	}
}
