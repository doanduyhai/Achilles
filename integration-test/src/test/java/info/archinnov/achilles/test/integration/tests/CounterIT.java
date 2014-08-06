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

import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CounterIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource("CompleteBean",
			AchillesCounter.ACHILLES_COUNTER_TABLE);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private CompleteBean bean;

	@Test
	public void should_persist_counter() throws Exception {
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(CounterBuilder.incr(2L)).buid();

		bean = manager.insert(bean);

		Row row = session.execute(
				"select counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

		assertThat(row.getLong("counter_value")).isEqualTo(2L);
	}

	@Test
	public void should_set_counter_on_managed_entity() throws Exception {
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.insert(bean);

		bean.getVersion().incr(2L);

		manager.update(bean);

		Row row = session.execute(
				"select counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

		assertThat(row.getLong("counter_value")).isEqualTo(2L);
	}

	@Test
	public void should_find_counter() throws Exception {
		long version = 10L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.insert(bean);
		bean.getVersion().incr(version);

		manager.update(bean);

		Row row = session.execute(
				"select counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

		assertThat(row.getLong("counter_value")).isEqualTo(version);
	}

	@Test
	public void should_remove_counter() throws Exception {
		long version = 154321L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.insert(bean);

		bean.getVersion().incr(version);

		manager.update(bean);

		Row row = session.execute(
				"select counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

		assertThat(row.getLong("counter_value")).isEqualTo(version);

		// Pause required to let Cassandra remove counter columns
		Thread.sleep(1000);

		manager.remove(bean);

		row = session.execute(
				"select counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

		assertThat(row).isNull();
	}

	@Test
	public void should_get_counter_from_raw_entity() throws Exception {
		CompleteBean bean = builder().randomId().version(CounterBuilder.incr(3L)).buid();

		assertThat(bean.getVersion().get()).isEqualTo(3L);
	}

	@Test
	public void should_get_counter_from_managed__entity_after_setting_value() throws Exception {
		CompleteBean bean = builder().randomId().buid();
		bean = manager.insert(bean);

		bean.getVersion().incr(5L);

		assertThat(bean.getVersion().get()).isEqualTo(5L);
	}

	@Test
	public void should_get_counter_from_refreshed_entity() throws Exception {
		CompleteBean bean = builder().randomId().buid();
		bean = manager.insert(bean);

		Counter version = bean.getVersion();
		version.incr(5L);

		manager.update(bean);

		assertThat(version.get()).isEqualTo(5L);

		manager.refresh(bean);

		assertThat(bean.getVersion().get()).isEqualTo(5L);
	}
}
