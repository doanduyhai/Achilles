package integration.tests;

import static info.archinnov.achilles.common.CassandraDaoTest.getConsistencyPolicy;
import static info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy.currentReadConsistencyLevel;
import static info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy.currentWriteConsistencyLevel;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import info.archinnov.achilles.exception.AchillesException;
import integration.tests.entity.BeanWithLocalQuorumConsistency;
import integration.tests.entity.BeanWithReadLocalQuorumConsistencyForExternalWidemap;
import integration.tests.entity.BeanWithReadOneWriteAllConsistencyForExternalWidemap;
import integration.tests.entity.BeanWithWriteLocalQuorumConsistencyForExternalWidemap;
import integration.tests.entity.BeanWithWriteOneAndReadLocalQuorumConsistency;

import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * ConsistencyLevelIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ConsistencyLevelIT
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private Long id = RandomUtils.nextLong();

	@Test
	public void should_throw_exception_when_persisting_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithLocalQuorumConsistency bean = new BeanWithLocalQuorumConsistency();
		bean.setId(id);
		bean.setName("name");

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("info.archinnov.achilles.exception.AchillesException: info.archinnov.achilles.exception.AchillesException: Error while executing the batch mutation : InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		em.persist(bean);
	}

	@Test
	public void should_throw_exception_when_loading_entity_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithWriteOneAndReadLocalQuorumConsistency bean = new BeanWithWriteOneAndReadLocalQuorumConsistency(
				id, "FN", "LN");

		em.persist(bean);

		expectedEx.expect(RuntimeException.class);
		expectedEx.expectMessage("Error when loading entity type '"
				+ BeanWithWriteOneAndReadLocalQuorumConsistency.class.getCanonicalName() + "'");

		em.find(BeanWithWriteOneAndReadLocalQuorumConsistency.class, id);
	}

	@Test
	public void should_insert_find_get_iterator_and_remove_for_widemap_with_consistency_level()
			throws Exception
	{
		BeanWithReadOneWriteAllConsistencyForExternalWidemap bean = new BeanWithReadOneWriteAllConsistencyForExternalWidemap(
				id, "name");

		bean = em.merge(bean);

		WideMap<Integer, String> wideMap = bean.getWideMap();

		wideMap.insert(1, "one");
		wideMap.insert(2, "two");
		wideMap.insert(3, "three");
		wideMap.insert(4, "four");

		List<KeyValue<Integer, String>> keyValues = wideMap.find(1, 3, 2);

		assertThat(keyValues).hasSize(2);
		assertThat(keyValues.get(0).getValue()).isEqualTo("one");
		assertThat(keyValues.get(1).getValue()).isEqualTo("two");

		List<Integer> keys = wideMap.findKeys(1, null, 2, BoundingMode.EXCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);
		assertThat(keys).hasSize(2);
		assertThat(keys).containsExactly(2, 3);

		List<String> values = wideMap.findValues(2, 3, 10);

		assertThat(values).hasSize(2);
		assertThat(values).containsExactly("two", "three");

		KeyValueIterator<Integer, String> iter = wideMap.iterator();

		assertThat(iter.next().getValue()).isEqualTo("one");
		assertThat(iter.next().getValue()).isEqualTo("two");
		assertThat(iter.next().getValue()).isEqualTo("three");
		assertThat(iter.next().getValue()).isEqualTo("four");

		wideMap.remove(1, 3);

		assertThat(wideMap.findValues(1, 5, 10)).containsExactly("four");
	}

	@Test
	public void should_exception_when_writing_to_widemap_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithWriteLocalQuorumConsistencyForExternalWidemap bean = new BeanWithWriteLocalQuorumConsistencyForExternalWidemap(
				id, "name");

		bean = em.merge(bean);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("info.archinnov.achilles.exception.AchillesException: Error while executing the batch mutation : InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		bean.getWideMap().insert(1, "one");
	}

	@Test
	public void should_exception_when_reading_from_widemap_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithReadLocalQuorumConsistencyForExternalWidemap bean = new BeanWithReadLocalQuorumConsistencyForExternalWidemap(
				id, "name");

		bean = em.merge(bean);

		WideMap<Integer, String> wideMap = bean.getWideMap();
		wideMap.insert(1, "one");
		wideMap.insert(2, "two");

		expectedEx.expect(RuntimeException.class);
		expectedEx
				.expectMessage("me.prettyprint.hector.api.exceptions.HInvalidRequestException: InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		wideMap.findValues(1, 5, 10);
	}

	@After
	public void cleanThreadLocals()
	{
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
		getConsistencyPolicy().reinitDefaultConsistencyLevel();
	}

	@AfterClass
	public static void cleanUp()
	{
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
		getConsistencyPolicy().reinitDefaultConsistencyLevel();
	}
}
