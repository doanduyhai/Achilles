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
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.interceptor.Event.POST_LOAD;
import static info.archinnov.achilles.interceptor.Event.POST_PERSIST;
import static info.archinnov.achilles.interceptor.Event.POST_REMOVE;
import static info.archinnov.achilles.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.interceptor.Event.PRE_PERSIST;
import static info.archinnov.achilles.interceptor.Event.PRE_REMOVE;
import static info.archinnov.achilles.interceptor.Event.PRE_UPDATE;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.entity.manager.PersistenceManagerFactory;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;

import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;

public class EventInterceptorIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

    private Interceptor<CompleteBean> prePersist = new Interceptor<CompleteBean>() {

        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setName("prePersist");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_PERSIST);
        }
    };

    private Interceptor<CompleteBean> postPersist = new Interceptor<CompleteBean>() {
        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setLabel("postPersist");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_PERSIST);

        }
    };

    private Interceptor<CompleteBean> preUpdate = new Interceptor<CompleteBean>() {
        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setName("preUpdate");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_UPDATE);
        }
    };

    private Interceptor<CompleteBean> postUpdate = new Interceptor<CompleteBean>() {
        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setLabel("postUpdate");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_UPDATE);
        }
    };

    private Interceptor<CompleteBean> preRemove = new Interceptor<CompleteBean>() {
        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setName("preRemove");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(PRE_REMOVE);
        }
    };

    private Interceptor<CompleteBean> postRemove = new Interceptor<CompleteBean>() {
        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setLabel("postRemove");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_REMOVE);
        }
    };

    private Interceptor<CompleteBean> postLoad = new Interceptor<CompleteBean>() {
        @Override
        public CompleteBean onEvent(CompleteBean entity) {
            entity.setLabel("postLoad");
            return entity;
        }

        @Override
        public List<Event> events() {
            return Arrays.asList(POST_LOAD);
        }
    };

    private List<Interceptor<CompleteBean>> interceptors = Arrays.asList(prePersist,postPersist,
                                            preUpdate,postUpdate,preRemove,postLoad);

    private List<Interceptor<CompleteBean>> postRemoveInterceptors = Arrays.asList(postRemove);

    private PersistenceManagerFactory pmf = CassandraEmbeddedServerBuilder
            .withEntityPackages(CompleteBean.class.getPackage().getName())
            .withKeyspaceName("interceptor_keyspace1")
            .withEventInterceptors(interceptors)
            .buildPersistenceManagerFactory();

    private PersistenceManager manager = pmf.createPersistenceManager();
    private Session session = manager.getNativeSession();

    private PersistenceManager manager2 = CassandraEmbeddedServerBuilder
            .withEntityPackages(CompleteBean.class.getPackage().getName())
            .withKeyspaceName("interceptor_keyspace2")
            .withEventInterceptors(postRemoveInterceptors)
            .buildPersistenceManager();
    private Session session2 = manager2.getNativeSession();


	@Test
	public void should_apply_persist_interceptors() throws Exception {

		CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

		manager.persist(entity);

		Row row = session.execute("select name,label from CompleteBean where id = "+ entity.getId()).one();

		assertThat(row.getString("name")).isEqualTo("prePersist");
		assertThat(row.getString("label")).isEqualTo("label");
		assertThat(entity.getName()).isEqualTo("prePersist");
		assertThat(entity.getLabel()).isEqualTo("postPersist");

	}

    @Test
    public void should_apply_merge_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        entity = manager.merge(entity);

        Row row = session.execute("select name,label from CompleteBean where id = "+ entity.getId()).one();

        assertThat(row.getString("name")).isEqualTo("preUpdate");
        assertThat(row.getString("label")).isEqualTo("label");
        assertThat(entity.getName()).isEqualTo("preUpdate");
        assertThat(entity.getLabel()).isEqualTo("postUpdate");
    }

    @Test
    public void should_apply_pre_remove_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.remove(entity);

        assertThat(entity.getName()).isEqualTo("preRemove");
    }

    @Test
    public void should_apply_post_remove_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager2.remove(entity);

        assertThat(entity.getLabel()).isEqualTo("postRemove");
    }

    @Test
    public void should_apply_post_load_interceptors() throws Exception {

        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.persist(entity);

        entity = manager.find(CompleteBean.class,entity.getId());

        assertThat(entity.getLabel()).isEqualTo("postLoad");
    }
}
