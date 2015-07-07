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

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.persistence.*;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.FORCE_TABLE_CREATION;
import static info.archinnov.achilles.interceptor.Event.*;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.CounterBuilder.incr;
import static info.archinnov.achilles.options.OptionsBuilder.withAsyncListeners;
import static info.archinnov.achilles.options.OptionsBuilder.withProxy;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;

public class AsyncEventInterceptorIT {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Interceptor<CompleteBean> prePersist = new Interceptor<CompleteBean>() {

        @Override
        public void onEvent(CompleteBean entity) {
            entity.setName("prePersist");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_INSERT);
        }
    };

    private Interceptor<CompleteBean> postPersist = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setLabel("postPersist : " + entity.getLabel());
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

    private Interceptor<CompleteBean> preRemove = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setName("preRemove");
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_DELETE);
        }
    };

    private Interceptor<CompleteBean> postRemove = new Interceptor<CompleteBean>() {
        @Override
        public void onEvent(CompleteBean entity) {
            entity.setLabel("postRemove");
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

    private List<Interceptor<CompleteBean>> interceptors = Arrays.asList(prePersist, postPersist, preUpdate,
            postUpdate, preRemove, postLoad);

    private List<Interceptor<CompleteBean>> postRemoveInterceptors = Arrays.asList(postRemove);

    private PersistenceManagerFactory pmf = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class)
            .cleanDataFilesAtStartup(true)
            .withKeyspaceName("async_interceptor_keyspace1")
            .withAchillesConfigParams(ImmutableMap.of(EVENT_INTERCEPTORS, interceptors, FORCE_TABLE_CREATION, true))
            .buildPersistenceManagerFactory();

    private AsyncManager asyncManager = pmf.createAsyncManager();
    private Session session = asyncManager.getNativeSession();

    private AsyncManager manager2 = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class)
            .cleanDataFilesAtStartup(true)
            .withKeyspaceName("async_interceptor_keyspace2")
            .withAchillesConfigParams(ImmutableMap.of(EVENT_INTERCEPTORS, postRemoveInterceptors, FORCE_TABLE_CREATION, true))
            .cleanDataFilesAtStartup(true)
            .buildPersistenceManagerFactory().createAsyncManager();

    private AsyncManager manager3 = CassandraEmbeddedServerBuilder
            .withEntities(ClusteredEntity.class)
            .cleanDataFilesAtStartup(true)
            .withKeyspaceName("async_interceptor_keyspace3")
            .withAchillesConfigParams(ImmutableMap.of(EVENT_INTERCEPTORS, asList(postLoadForClustered), FORCE_TABLE_CREATION, true))
            .cleanDataFilesAtStartup(true)
            .buildPersistenceManagerFactory().createAsyncManager();

    @Test
    public void should_apply_persist_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").version(incr(2L)).buid();

        final CountDownLatch latch = new CountDownLatch(1);
        asyncManager.insert(entity, withAsyncListeners(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }));

        latch.await();

        assertThat(entity.getName()).isEqualTo("prePersist");
        assertThat(entity.getLabel()).isEqualTo("postPersist : label");

        Row row = session.execute("select name,label from CompleteBean where id = " + entity.getId()).one();

        assertThat(row.getString("name")).isEqualTo("prePersist");
        assertThat(row.getString("label")).isEqualTo("label");

    }

    @Test
    public void should_apply_update_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().buid();

        final CountDownLatch latch = new CountDownLatch(2);
        final AchillesFuture<CompleteBean> future = asyncManager.insert(entity, withAsyncListeners(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }));


        future.get();

        final CompleteBean proxy = asyncManager.find(CompleteBean.class, entity.getId(), withProxy()).getImmediately();
        proxy.setName("DuyHai");
        proxy.setLabel("label");


        asyncManager.update(proxy, withAsyncListeners(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }));

        latch.await();

        Row row = session.execute("select name,label from CompleteBean where id = " + entity.getId()).one();

        assertThat(row.getString("name")).isEqualTo("preUpdate");
        assertThat(row.getString("label")).isEqualTo("label");
        assertThat(proxy.getName()).isEqualTo("preUpdate");
        assertThat(proxy.getLabel()).isEqualTo("postUpdate");
    }

    @Test
    public void should_apply_pre_delete_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        asyncManager.delete(entity);

        assertThat(entity.getName()).isEqualTo("preRemove");
    }

    @Test
    public void should_apply_post_delete_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        final CountDownLatch latch = new CountDownLatch(1);
        manager2.delete(entity, withAsyncListeners(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }));

        latch.await();

        assertThat(entity.getLabel()).isEqualTo("postRemove");
    }

    @Test
    public void should_apply_post_load_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        asyncManager.insert(entity).getImmediately();

        final CountDownLatch latch = new CountDownLatch(1);

        final AchillesFuture<CompleteBean> future = asyncManager.find(CompleteBean.class, entity.getId(), withAsyncListeners(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }));

        latch.await();

        assertThat(future.get().getLabel()).isEqualTo("postLoad");
    }

    @Test
    public void should_apply_interceptors_after_flush_for_batch() throws Exception {
        // Given
        final AsyncBatch asyncBatch = asyncManager.createLoggedBatch();
        asyncBatch.startBatch();

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        // When
        asyncBatch.insert(entity);

        // Then
        assertThat(entity.getName()).isEqualTo("DuyHai");
        assertThat(entity.getLabel()).isEqualTo("label");

        // When
        final CountDownLatch latch = new CountDownLatch(1);
        asyncBatch.asyncEndBatch(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });

        latch.await();

        // Then
        assertThat(entity.getName()).isEqualTo("prePersist");

        assertThat(entity.getLabel()).isEqualTo("postPersist : label");
    }

    @Test
    public void should_apply_post_load_interceptor_on_slice_query() throws Exception {
        // Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        Integer count = RandomUtils.nextInt(0,Integer.MAX_VALUE);
        String name = RandomStringUtils.randomAlphabetic(10);
        String value = "value_before_load";
        ClusteredEntity entity = new ClusteredEntity(id, count, name, value);

        manager3.insert(entity).getImmediately();

        // When
        final AchillesFuture<List<ClusteredEntity>> futures = manager3.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(id)
                .get(10);


        // Then
        while (!futures.isDone()) {
            Thread.sleep(4);
        }
        assertThat(futures.get().get(0).getValue()).isEqualTo("postLoad");
    }

    @Test
    public void should_apply_post_load_interceptor_on_typed_query() throws Exception {
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        asyncManager.insert(entity).getImmediately();

        RegularStatement statement = select().from("CompleteBean").where(eq("id",bindMarker()));

        // When
        final CountDownLatch latch = new CountDownLatch(1);
        final AchillesFuture<CompleteBean> future = asyncManager.typedQuery(CompleteBean.class, statement, entity.getId()).getFirst(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });

        latch.await();

        // Then
        assertThat(future.get().getLabel()).isEqualTo("postLoad");
    }

    @Test
    public void should_apply_post_load_interceptor_on_raw_typed_query() throws Exception {
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();


        asyncManager.insert(entity).getImmediately();

        RegularStatement statement = select().from("CompleteBean").where(eq("id",bindMarker()));

        // When
        final CountDownLatch latch = new CountDownLatch(1);

        final AchillesFuture<CompleteBean> future = asyncManager.typedQuery(CompleteBean.class, statement, entity.getId()).getFirst(new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });

        latch.await();

        // Then
        assertThat(future.get().getLabel()).isEqualTo("postLoad");
    }
}
