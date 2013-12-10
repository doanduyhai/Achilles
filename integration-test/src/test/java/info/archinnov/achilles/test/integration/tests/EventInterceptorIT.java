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

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.EventInterceptor;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;

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

	@Test
	public void should_apply_entity_interceptors() throws Exception {

		EventInterceptor<CompleteBean> prepersitInterceptor = new EventInterceptor<CompleteBean>() {

			@Override
			public CompleteBean onEvent(CompleteBean entity) {
				entity.setAge(15L);
				return entity;
			}

			@Override
			public List<Event> events() {
				return new ImmutableList.Builder<Event>().add(Event.PRE_PERSIST).build();

			}

		};
		EventInterceptor<CompleteBean> postPersistInterceptor = new EventInterceptor<CompleteBean>() {

			@Override
			public CompleteBean onEvent(CompleteBean entity) {
				entity.setLabel("testUpdated");
				return entity;
			}

			@Override
			public List<Event> events() {
				return new ImmutableList.Builder<Event>().add(Event.POST_PERSIST).build();

			}

		};
		ImmutableList<EventInterceptor<?>> eventInterceptors = new ImmutableList.Builder<EventInterceptor<?>>()
				.add(prepersitInterceptor).add(postPersistInterceptor).build();
		PersistenceManager manager = CassandraEmbeddedServerBuilder
				.withEntityPackages(CompleteBean.class.getPackage().getName()).withKeyspaceName("my_keyspace")
				.withEventInterceptors(eventInterceptors).buildPersistenceManager();
		Session session = manager.getNativeSession();
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L).label("test")
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();

		manager.persist(entity);

		Row row = session.execute(
				"select name,age_in_years,label,friends,followers,preferences from completebean where id = "
						+ entity.getId()).one();

		assertThat(row.getLong("age_in_years")).isEqualTo(15L);
		assertThat(row.getString("label")).isEqualTo("test");
		assertThat(entity.getLabel()).isEqualTo("testUpdated");

	}
}
