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

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithLocalQuorumConsistency;
import info.archinnov.achilles.test.integration.entity.EntityWithWriteOneAndReadLocalQuorumConsistency;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnavailableException;

public class ConsistencyLevelIT {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
			EntityWithLocalQuorumConsistency.TABLE_NAME, EntityWithWriteOneAndReadLocalQuorumConsistency.TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private Long id = RandomUtils.nextLong();

	@Test
	public void should_throw_exception_when_persisting_with_local_quorum_consistency() throws Exception {
		EntityWithLocalQuorumConsistency bean = new EntityWithLocalQuorumConsistency();
		bean.setId(id);
		bean.setName("name");

		expectedEx.expect(InvalidQueryException.class);
		expectedEx
				.expectMessage("consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");

		manager.persist(bean);
	}

	@Test
	public void should_throw_exception_when_loading_entity_with_local_quorum_consistency() throws Exception {
		EntityWithWriteOneAndReadLocalQuorumConsistency bean = new EntityWithWriteOneAndReadLocalQuorumConsistency(id,
				"FN", "LN");

		manager.persist(bean);

		expectedEx.expect(DriverInternalError.class);
		expectedEx
				.expectMessage("An unexpected error occured server side on localhost/127.0.0.1: java.lang.ClassCastException: org.apache.cassandra.locator.SimpleStrategy cannot be cast to org.apache.cassandra.locator.NetworkTopologyStrategy");

		manager.find(EntityWithWriteOneAndReadLocalQuorumConsistency.class, id);
	}

	@Test
	public void should_recover_from_exception_and_reinit_consistency_level() throws Exception {
		EntityWithWriteOneAndReadLocalQuorumConsistency bean = new EntityWithWriteOneAndReadLocalQuorumConsistency(id,
				"FN", "LN");

		try {
			manager.persist(bean);
			manager.find(EntityWithWriteOneAndReadLocalQuorumConsistency.class, id);
		} catch (DriverInternalError e) {
			// Should recover from exception
		}
		CompleteBean newBean = new CompleteBean();
		newBean.setId(id);
		newBean.setName("name");

		manager.persist(newBean);

		newBean = manager.find(CompleteBean.class, newBean.getId());

		assertThat(newBean).isNotNull();
		assertThat(newBean.getName()).isEqualTo("name");
	}

	@Test
	public void should_persist_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name zerferg").buid();

		try {
			manager.persist(entity, OptionsBuilder.withConsistency(EACH_QUORUM));
		} catch (InvalidQueryException e) {
			assertThat(e)
					.hasMessage(
							"consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
		}

		logAsserter.prepareLogLevel();
		manager.persist(entity, OptionsBuilder.withConsistency(ALL));
		CompleteBean found = manager.find(CompleteBean.class, entity.getId());
		assertThat(found.getName()).isEqualTo("name zerferg");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_update_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name zeruioze").buid();

		try {
			entity = manager.persist(entity);
            entity.setName("zeruioze");
            manager.update(entity);
		} catch (InvalidQueryException e) {
			assertThat(e)
					.hasMessage(
							"consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
		}

		logAsserter.prepareLogLevel();
		manager.update(entity, OptionsBuilder.withConsistency(ALL));
		CompleteBean found = manager.find(CompleteBean.class, entity.getId());
		assertThat(found.getName()).isEqualTo("zeruioze");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_find_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name rtprt").buid();
		manager.persist(entity);

		try {
			manager.find(CompleteBean.class, entity.getId(), ConsistencyLevel.EACH_QUORUM);
		} catch (InvalidQueryException e) {
			assertThat(e).hasMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");
		}
		logAsserter.prepareLogLevel();
		CompleteBean found = manager.find(CompleteBean.class, entity.getId(), ConsistencyLevel.ALL);
		assertThat(found.getName()).isEqualTo("name rtprt");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_refresh_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);

		try {
			manager.refresh(entity, ConsistencyLevel.EACH_QUORUM);
		} catch (InvalidQueryException e) {
			assertThat(e).hasMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");
		}
		logAsserter.prepareLogLevel();
		manager.refresh(entity, ConsistencyLevel.ALL);
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_remove_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);

		try {
			manager.remove(entity, OptionsBuilder.withConsistency(ConsistencyLevel.EACH_QUORUM));
		} catch (InvalidQueryException e) {
			assertThat(e)
					.hasMessage(
							"consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
		}
		logAsserter.prepareLogLevel();
		manager.remove(entity, OptionsBuilder.withConsistency(ConsistencyLevel.ALL));
		assertThat(manager.find(CompleteBean.class, entity.getId())).isNull();
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_reinit_consistency_level_after_exception() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name qzerferf").buid();
		try {
			manager.persist(entity, OptionsBuilder.withConsistency(EACH_QUORUM));
		} catch (InvalidQueryException e) {
			assertThat(e)
					.hasMessage(
							"consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
		}
		logAsserter.prepareLogLevel();
		manager.persist(entity, OptionsBuilder.withConsistency(ALL));
		CompleteBean found = manager.find(CompleteBean.class, entity.getId());
		assertThat(found.getName()).isEqualTo("name qzerferf");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_get_counter_with_consistency_level() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);
		try {
			entity.getVersion().get(ConsistencyLevel.EACH_QUORUM);
		} catch (InvalidQueryException e) {
			assertThat(e).hasMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");
		}
	}

	@Test
	public void should_increment_counter_with_consistency_level() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);
		try {
			entity.getVersion().incr(ConsistencyLevel.THREE);
		} catch (UnavailableException e) {
			assertThat(e).hasMessage(
					"Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
		}
	}

	@Test
	public void should_increment_n_counter_with_consistency_level() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);
		try {
			entity.getVersion().incr(10L, ConsistencyLevel.THREE);
		} catch (UnavailableException e) {
			assertThat(e).hasMessage(
					"Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
		}
	}

	@Test
	public void should_decrement_counter_with_consistency_level() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);
		try {
			entity.getVersion().decr(ConsistencyLevel.THREE);
		} catch (UnavailableException e) {
			assertThat(e).hasMessage(
					"Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
		}
	}

	@Test
	public void should_decrement_counter_n_with_consistency_level() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = manager.persist(entity);
		try {
			entity.getVersion().decr(10L, ConsistencyLevel.THREE);
		} catch (UnavailableException e) {
			assertThat(e).hasMessage(
					"Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
		}
	}

}
