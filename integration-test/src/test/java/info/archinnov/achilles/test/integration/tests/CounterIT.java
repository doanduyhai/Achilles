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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;

public class CounterIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource("CompleteBean",
			AchillesCounter.CQL_COUNTER_TABLE);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private CompleteBean bean;

	@Test
	public void should_persist_counter() throws Exception {
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.persist(bean);
		bean.getVersion().incr(2L);

		Row row = session.execute(
				"select counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

		assertThat(row.getLong("counter_value")).isEqualTo(2L);
	}

	@Test
	public void should_find_counter() throws Exception {
		long version = 10L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.persist(bean);
		bean.getVersion().incr(version);

		assertThat(bean.getVersion().get()).isEqualTo(version);
	}

	@Test
	public void should_remove_counter() throws Exception {
		long version = 154321L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = manager.persist(bean);
		bean.getVersion().incr(version);

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
}
