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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.THREE;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.common.base.Optional;
import info.archinnov.achilles.context.CQLBatchingFlushContext;
import info.archinnov.achilles.entity.manager.CQLBatchingPersistenceManager;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.entity.manager.CQLPersistenceManagerFactory;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.EntityWithConsistencyLevelOnClassAndField;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

public class ConsistencyLevelPriorityOrderingIT {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private CQLPersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	// Normal type
	@Test
	public void should_override_mapping_on_class_by_runtime_value_on_batch_mode_for_normal_type() throws Exception {
		EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
		long id = RandomUtils.nextLong();
		entity.setId(id);
		entity.setName("name");

		manager.persist(entity);

		CQLBatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
		batchEm.startBatch(ONE);
		logAsserter.prepareLogLevel();

		entity = batchEm.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());

		logAsserter.assertConsistencyLevels(ONE, ONE);
		batchEm.endBatch();

		assertThatBatchContextHasBeenReset(batchEm);
		assertThat(entity.getName()).isEqualTo("name");

		expectedEx.expect(InvalidQueryException.class);
		expectedEx
				.expectMessage("consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
		manager.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());
	}

	@Test
	public void should_not_override_batch_mode_level_by_runtime_value_for_normal_type() throws Exception {
		EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name sdfsdf");
		manager.persist(entity);

		CQLBatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();

		batchEm.startBatch(EACH_QUORUM);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		entity = batchEm.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId(), ONE);
	}

	// Counter type
	@Test
	public void should_override_mapping_on_class_by_mapping_on_field_for_counter_type() throws Exception {
		EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = manager.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);

		logAsserter.prepareLogLevel();
		assertThat(counter.get()).isEqualTo(10L);
		logAsserter.assertConsistencyLevels(ONE, ONE);
	}

	@Test
	public void should_override_mapping_on_field_by_batch_value_for_counter_type() throws Exception {
		EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		CQLBatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
		batchEm.startBatch(THREE);
		entity = batchEm.merge(entity);

		Counter counter = entity.getCount();

		expectedEx.expect(UnavailableException.class);
		expectedEx
				.expectMessage("Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
		counter.incr(10L);
	}

	@Test
	public void should_override_mapping_on_field_by_runtime_value_for_counter_type() throws Exception {
		EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = manager.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);

		expectedEx.expect(InvalidQueryException.class);
		expectedEx.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

		counter.get(EACH_QUORUM);
	}

	@Test
	public void should_override_batch_level_by_runtime_value_for_counter_type() throws Exception {
		EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		CQLBatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
		batchEm.startBatch(ONE);
		entity = batchEm.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);

		expectedEx.expect(InvalidQueryException.class);
		expectedEx.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

		counter.get(EACH_QUORUM);
	}

	@Test
	public void should_override_batch_level_by_runtime_value_for_slice_query() throws Exception {

		CQLBatchingPersistenceManager batchEm = pmf.createBatchingPersistenceManager();
		batchEm.startBatch(ONE);

		expectedEx.expect(InvalidQueryException.class);
		expectedEx.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

		batchEm.sliceQuery(ClusteredEntity.class).partitionComponents(11L).consistencyLevel(EACH_QUORUM).get(10);
	}

	private void assertThatBatchContextHasBeenReset(CQLBatchingPersistenceManager batchEm) {
		CQLBatchingFlushContext flushContext = Whitebox.getInternalState(batchEm, CQLBatchingFlushContext.class);
		Optional<ConsistencyLevel> consistencyLevel = Whitebox.getInternalState(flushContext, "consistencyLevel");
		List<BoundStatementWrapper> boundStatementWrappers = Whitebox.getInternalState(flushContext,
				"boundStatementWrappers");

		assertThat(consistencyLevel).isNull();
		assertThat(boundStatementWrappers).isEmpty();
	}

}
