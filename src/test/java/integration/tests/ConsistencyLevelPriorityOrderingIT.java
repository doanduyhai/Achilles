package integration.tests;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftBatchingEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.wrapper.CounterBuilder;
import integration.tests.entity.BeanWithConsistencyLevelOnClassAndField;
import integration.tests.utils.CassandraLogAsserter;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * ConsistencyLevelPriorityIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ConsistencyLevelPriorityOrderingIT
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private ThriftConsistencyLevelPolicy policy = ThriftCassandraDaoTest
			.getConsistencyPolicy();

	// Normal type
	@Test
	public void should_override_mapping_on_class_by_runtime_value_on_batch_mode_for_normal_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		em.persist(entity);

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch(ONE, ONE);
		logAsserter.prepareLogLevel();

		entity = batchEm.find(BeanWithConsistencyLevelOnClassAndField.class, entity.getId());

		logAsserter.assertConsistencyLevels(ONE, ONE);
		batchEm.endBatch();

		assertThatConsistencyLevelsAreReinitialized();
		assertThat(entity.getName()).isEqualTo("name");

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("Error when loading entity type 'integration.tests.entity.BeanWithConsistencyLevelOnClassAndField' with key '"
						+ entity.getId()
						+ "'. Cause : InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		em.find(BeanWithConsistencyLevelOnClassAndField.class, entity.getId());
	}

	@Test
	public void should_not_override_batch_mode_level_by_runtime_value_for_normal_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name sdfsdf");
		em.persist(entity);

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();

		batchEm.startBatch(EACH_QUORUM, EACH_QUORUM);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		entity = batchEm.find(BeanWithConsistencyLevelOnClassAndField.class, entity.getId(), ONE);
	}

	// WideMap type
	@Test
	public void should_override_mapping_on_class_by_mapping_on_field_for_widemap_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		entity = em.merge(entity);
		WideMap<Integer, String> widemap = entity.getWideMap();
		widemap.insert(10, "10");

		logAsserter.prepareLogLevel();
		assertThat(widemap.get(10)).isEqualTo("10");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM);
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_override_mapping_on_field_by_batch_mode_value_for_widemap_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());

		entity = em.merge(entity);

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch(ONE, ONE);
		logAsserter.prepareLogLevel();

		entity = batchEm.find(BeanWithConsistencyLevelOnClassAndField.class, entity.getId());
		WideMap<Integer, String> wideMapEachQuorumWrite = entity.getWideMapEachQuorumWrite();
		wideMapEachQuorumWrite.insert(10, "10");

		batchEm.endBatch();
		logAsserter.assertConsistencyLevels(ONE, ONE);
		assertThatConsistencyLevelsAreReinitialized();

		assertThat(wideMapEachQuorumWrite.get(10)).isEqualTo("10");

		entity = em.merge(entity);

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		entity.getWideMapEachQuorumWrite().insert(10, "10");
	}

	@Test
	public void should_override_mapping_on_fields_by_runtime_value_on_widemap_api_for_widemap_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);
		WideMap<Integer, String> wideMapEachQuorumWrite = entity.getWideMapEachQuorumWrite();

		wideMapEachQuorumWrite.insert(10, "10", ConsistencyLevel.ONE);

		assertThat(wideMapEachQuorumWrite.get(10)).isEqualTo("10");

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		wideMapEachQuorumWrite.insert(10, "10");
	}

	@Test
	public void should_not_override_batch_mode_level_by_runtime_value_on_widemap_api_for_widemap_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch(ONE, EACH_QUORUM);
		entity = batchEm.merge(entity);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		entity.getWideMap().insert(10, "10", ONE);
	}

	// Counter type
	@Test
	public void should_override_mapping_on_class_by_mapping_on_field_for_counter_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);

		logAsserter.prepareLogLevel();
		assertThat(counter.get()).isEqualTo(10L);
		logAsserter.assertConsistencyLevels(ONE, ONE);
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_override_mapping_on_field_by_batch_value_for_counter_type() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch(EACH_QUORUM, ONE);
		entity = batchEm.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");

		counter.get();

	}

	@Test
	public void should_override_mapping_on_field_by_runtime_value_for_counter_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");

		counter.get(EACH_QUORUM);
	}

	@Test
	public void should_not_override_batch_level_by_runtime_value_for_counter_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch(ONE, ONE);
		entity = batchEm.merge(entity);

		Counter counter = entity.getCount();
		counter.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");

		counter.get(EACH_QUORUM);
	}

	// Counter WideMap type
	@Test
	public void should_override_mapping_on_class_by_runtime_value_for_counter_widemap_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");

		entity = em.merge(entity);

		entity.getCounterWideMap().insert(11, CounterBuilder.incr());
		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		entity.getCounterWideMap().get(11).incr(EACH_QUORUM);
	}

	@Test
	public void should_override_mapping_on_class_by_batch_value_for_counter_widemap_type()
			throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);

		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch(EACH_QUORUM, EACH_QUORUM);
		entity = batchEm.merge(entity);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("me.prettyprint.hector.api.exceptions.HInvalidRequestException: InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		entity.getCounterWideMap().insert(11, CounterBuilder.incr());
	}

	private void assertThatConsistencyLevelsAreReinitialized()
	{
		assertThat(policy.getCurrentReadLevel()).isNull();
		assertThat(policy.getCurrentWriteLevel()).isNull();
	}

}
