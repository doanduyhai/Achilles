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
package info.archinnov.achilles.test.integration.tests;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.interceptor.Event.POST_LOAD;
import static info.archinnov.achilles.interceptor.Event.POST_INSERT;
import static info.archinnov.achilles.interceptor.Event.POST_DELETE;
import static info.archinnov.achilles.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.interceptor.Event.PRE_INSERT;
import static info.archinnov.achilles.interceptor.Event.PRE_DELETE;
import static info.archinnov.achilles.interceptor.Event.PRE_UPDATE;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.CounterBuilder.incr;
import static info.archinnov.achilles.type.OptionsBuilder.withProxy;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.List;

import info.archinnov.achilles.test.integration.entity.ChildEntity;
import info.archinnov.achilles.test.integration.entity.ParentEntity;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.persistence.Batch;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompleteBean;

public class EventInterceptorIT {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Interceptor<CompleteBean> preInsert = new Interceptor<CompleteBean>() {

        @Override
        public void onEvent(CompleteBean entity) {
            entity.setName("preInsert");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_INSERT);
        }
    };

    private Interceptor<CompleteBean> postInsert = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setLabel("postInsert : " + entity.getLabel());
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_INSERT);

        }
    };

    private Interceptor<CompleteBean> preUpdate = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setName("preUpdate");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_UPDATE);
        }
    };

    private Interceptor<CompleteBean> postUpdate = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setLabel("postUpdate");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_UPDATE);
        }
    };

    private Interceptor<CompleteBean> preDelete = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setName("preDelete");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_DELETE);
        }
    };

    private Interceptor<CompleteBean> postDelete = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setLabel("postDelete");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_DELETE);
        }
    };

    private Interceptor<CompleteBean> postLoad = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setLabel("postLoad");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_LOAD);
        }
    };

    private Interceptor<ClusteredEntity> postLoadForClustered = new Interceptor<ClusteredEntity>() {
        @Override
        public void onEvent(ClusteredEntity entity) {
            entity.setValue("postLoad");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_LOAD);
        }
    };

    private Interceptor<ParentEntity> postInsertParentEntity = new Interceptor<ParentEntity>() {
        @Override
        public void onEvent(ParentEntity entity) {
            entity.setParentValue("post_insert_parent_and_children");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_INSERT);
        }
    };

    private List<Interceptor<?>> interceptors = Arrays.asList(preInsert, postInsert, preUpdate,
            postUpdate, preDelete, postLoad, postInsertParentEntity);

    private List<Interceptor<CompleteBean>> postRemoveInterceptors = Arrays.asList(postDelete);

    private PersistenceManagerFactory pmf = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class, ParentEntity.class, ChildEntity.class)
            .cleanDataFilesAtStartup(true)
            .withKeyspaceName("interceptor_keyspace1")
            .withAchillesConfigParams(ImmutableMap.of(EVENT_INTERCEPTORS, interceptors, FORCE_TABLE_CREATION, true))
            .buildPersistenceManagerFactory();

    private PersistenceManager manager = pmf.createPersistenceManager();
    private Session session = manager.getNativeSession();

    private PersistenceManager manager2 = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class)
            .cleanDataFilesAtStartup(true)
            .withKeyspaceName("interceptor_keyspace2")
            .withAchillesConfigParams(ImmutableMap.of(EVENT_INTERCEPTORS, postRemoveInterceptors, FORCE_TABLE_CREATION, true))
            .cleanDataFilesAtStartup(true)
            .buildPersistenceManager();

    private PersistenceManager manager3 = CassandraEmbeddedServerBuilder
            .withEntities(ClusteredEntity.class)
            .cleanDataFilesAtStartup(true)
            .withKeyspaceName("interceptor_keyspace3")
            .withAchillesConfigParams(ImmutableMap.of(EVENT_INTERCEPTORS, asList(postLoadForClustered), FORCE_TABLE_CREATION, true))
            .cleanDataFilesAtStartup(true)
            .buildPersistenceManager();

    @Test
    public void should_apply_insert_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").version(incr(2L)).buid();

        manager.insert(entity);

        assertThat(entity.getName()).isEqualTo("preInsert");
        assertThat(entity.getLabel()).isEqualTo("postInsert : label");

        Row row = session.execute("select name,label from CompleteBean where id = " + entity.getId()).one();

        assertThat(row.getString("name")).isEqualTo("preInsert");
        assertThat(row.getString("label")).isEqualTo("label");
    }

    @Test
    public void should_apply_insert_interceptors_for_parent_and_children() throws Exception {
        //Given
        ParentEntity parentEntity = new ParentEntity(10L, "parentValue");
        ChildEntity childEntity = new ChildEntity(10L, "parentValue", "childValue");

        //When
        manager.insert(parentEntity);
        manager.insert(childEntity);

        Row rowParent = session.execute("select parent_value from parent_entity where id = " + parentEntity.getId()).one();
        Row rowChild = session.execute("select parent_value from child_entity where id = " + childEntity.getId()).one();

        //Then
        assertThat(parentEntity.getParentValue()).isEqualTo("post_insert_parent_and_children");
        assertThat(childEntity.getParentValue()).isEqualTo("post_insert_parent_and_children");
        assertThat(rowParent.getString("parent_value")).isEqualTo("parentValue");
        assertThat(rowChild.getString("parent_value")).isEqualTo("parentValue");
    }

    @Test
    public void should_apply_update_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().buid();

        manager.insert(entity);

        CompleteBean proxy = manager.find(CompleteBean.class, entity.getId(), withProxy());

        proxy.setName("DuyHai");
        proxy.setLabel("label");

        manager.update(proxy);

        Row row = session.execute("select name,label from CompleteBean where id = " + proxy.getId()).one();

        assertThat(row.getString("name")).isEqualTo("preUpdate");
        assertThat(row.getString("label")).isEqualTo("label");
        assertThat(proxy.getName()).isEqualTo("preUpdate");
        assertThat(proxy.getLabel()).isEqualTo("postUpdate");
    }

    @Test
    public void should_apply_pre_delete_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.delete(entity);

        assertThat(entity.getName()).isEqualTo("preDelete");
    }

    @Test
    public void should_apply_post_delete_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager2.delete(entity);

        assertThat(entity.getLabel()).isEqualTo("postDelete");
    }

    @Test
    public void should_apply_post_load_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.insert(entity);

        entity = manager.find(CompleteBean.class, entity.getId());

        assertThat(entity.getLabel()).isEqualTo("postLoad");
    }

    @Test
    public void should_apply_interceptors_after_flush_for_batch() throws Exception {
        // Given
        final Batch batchingPM = pmf.createLoggedBatch();
        batchingPM.startBatch();

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        // When
        batchingPM.insert(entity);

        // Then
        assertThat(entity.getName()).isEqualTo("DuyHai");
        assertThat(entity.getLabel()).isEqualTo("label");

        // When
        batchingPM.endBatch();

        // Then
        assertThat(entity.getName()).isEqualTo("preInsert");
        assertThat(entity.getLabel()).isEqualTo("postInsert : label");
    }

    @Test
    public void should_apply_post_load_interceptor_on_slice_query() throws Exception {
        // Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        Integer count = RandomUtils.nextInt(0,Integer.MAX_VALUE);
        String name = RandomStringUtils.randomAlphabetic(10);
        String value = "value_before_load";
        ClusteredEntity entity = new ClusteredEntity(id, count, name, value);

        manager3.insert(entity);

        // When
        final List<ClusteredEntity> clusteredEntities = manager3.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(id)
                .get(10);

        // Then
        assertThat(clusteredEntities.get(0).getValue()).isEqualTo("postLoad");
    }

    @Test
    public void should_apply_post_load_interceptor_on_typed_query() throws Exception {
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.insert(entity);

        RegularStatement statement = select().from("CompleteBean").where(eq("id",bindMarker()));

        // When
        final CompleteBean actual = manager.typedQuery(CompleteBean.class, statement,entity.getId()).getFirst();

        // Then
        assertThat(actual.getLabel()).isEqualTo("postLoad");
    }

    @Test
    public void should_apply_post_load_interceptor_on_raw_typed_query() throws Exception {
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.insert(entity);

        RegularStatement statement = select().from("CompleteBean").where(eq("id",bindMarker()));

        // When
        final CompleteBean actual = manager.typedQuery(CompleteBean.class, statement,entity.getId()).getFirst();

        // Then
        assertThat(actual.getLabel()).isEqualTo("postLoad");
    }
}
