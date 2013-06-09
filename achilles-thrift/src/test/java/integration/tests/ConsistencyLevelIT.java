package integration.tests;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftBatchingEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.WideMap;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;
import integration.tests.entity.BeanWithConsistencyLevelOnClassAndField;
import integration.tests.entity.BeanWithLocalQuorumConsistency;
import integration.tests.entity.BeanWithReadLocalQuorumConsistencyForWidemap;
import integration.tests.entity.BeanWithReadOneWriteAllConsistencyForWidemap;
import integration.tests.entity.BeanWithWriteLocalQuorumConsistencyForWidemap;
import integration.tests.entity.BeanWithWriteOneAndReadLocalQuorumConsistency;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;
import integration.tests.entity.User;
import integration.tests.utils.CassandraLogAsserter;

import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.TweetTestBuilder;
import testBuilders.UserTestBuilder;

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

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private Cluster cluster = ThriftCassandraDaoTest.getCluster();

	private String keyspaceName = ThriftCassandraDaoTest.getKeyspace().getKeyspaceName();

	private ThriftGenericWideRowDao counterWideMapDao = ThriftCassandraDaoTest.getColumnFamilyDao(
			"counter_widemap", Long.class, Long.class);

	private Long id = RandomUtils.nextLong();

	private ThriftConsistencyLevelPolicy policy = ThriftCassandraDaoTest.getConsistencyPolicy();

	@Test
	public void should_throw_exception_when_persisting_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithLocalQuorumConsistency bean = new BeanWithLocalQuorumConsistency();
		bean.setId(id);
		bean.setName("name");

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		em.persist(bean);
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_throw_exception_when_loading_entity_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithWriteOneAndReadLocalQuorumConsistency bean = new BeanWithWriteOneAndReadLocalQuorumConsistency(
				id, "FN", "LN");

		em.persist(bean);

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("Error when loading entity type '"
				+ BeanWithWriteOneAndReadLocalQuorumConsistency.class.getCanonicalName() + "'");

		em.find(BeanWithWriteOneAndReadLocalQuorumConsistency.class, id);
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_insert_find_get_iterator_and_remove_for_widemap_with_consistency_level()
			throws Exception
	{
		BeanWithReadOneWriteAllConsistencyForWidemap bean = new BeanWithReadOneWriteAllConsistencyForWidemap(
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
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_exception_when_writing_to_widemap_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithWriteLocalQuorumConsistencyForWidemap bean = new BeanWithWriteLocalQuorumConsistencyForWidemap(
				id, "name");

		bean = em.merge(bean);

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		bean.getWideMap().insert(1, "one");
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_exception_when_reading_from_widemap_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithReadLocalQuorumConsistencyForWidemap bean = new BeanWithReadLocalQuorumConsistencyForWidemap(
				id, "name");

		bean = em.merge(bean);

		WideMap<Integer, String> wideMap = bean.getWideMap();
		wideMap.insert(1, "one");
		wideMap.insert(2, "two");

		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		wideMap.findValues(1, 5, 10);
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_recover_from_exception_and_reinit_consistency_level() throws Exception
	{
		BeanWithWriteOneAndReadLocalQuorumConsistency bean = new BeanWithWriteOneAndReadLocalQuorumConsistency(
				id, "FN", "LN");

		try
		{
			em.persist(bean);
			em.find(BeanWithWriteOneAndReadLocalQuorumConsistency.class, id);
		}
		catch (AchillesException e)
		{
			// Should reinit consistency level to default
		}
		BeanWithReadOneWriteAllConsistencyForWidemap newBean = new BeanWithReadOneWriteAllConsistencyForWidemap(
				id, "name");

		em.persist(newBean);

		newBean = em.find(BeanWithReadOneWriteAllConsistencyForWidemap.class, newBean.getId());

		assertThat(newBean).isNotNull();
		assertThat(newBean.getName()).isEqualTo("name");
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_persist_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("name zerferg")
				.buid();

		try
		{
			em.persist(entity, ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}

		assertThatConsistencyLevelsAreReinitialized();

		logAsserter.prepareLogLevel();
		em.persist(entity, ConsistencyLevel.ALL);
		CompleteBean found = em.find(CompleteBean.class, entity.getId());
		assertThat(found.getName()).isEqualTo("name zerferg");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_merge_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("name zeruioze")
				.buid();

		try
		{
			em.merge(entity, ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();

		logAsserter.prepareLogLevel();
		em.merge(entity, ConsistencyLevel.ALL);
		CompleteBean found = em.find(CompleteBean.class, entity.getId());
		assertThat(found.getName()).isEqualTo("name zeruioze");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_find_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("name rtprt")
				.buid();
		em.persist(entity);

		try
		{
			em.find(CompleteBean.class, entity.getId(), ConsistencyLevel.EACH_QUORUM);
		}
		catch (AchillesException e)
		{
			assertThat(e)
					.hasMessage(
							"Error when loading entity type '"
									+ CompleteBean.class.getCanonicalName()
									+ "' with key '"
									+ entity.getId()
									+ "'. Cause : InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}
		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		CompleteBean found = em.find(CompleteBean.class, entity.getId(), ConsistencyLevel.ALL);
		assertThat(found.getName()).isEqualTo("name rtprt");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_refresh_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);

		try
		{
			em.refresh(entity, ConsistencyLevel.EACH_QUORUM);
		}
		catch (AchillesException e)
		{
			assertThat(e)
					.hasMessage(
							"Error when loading entity type '"
									+ CompleteBean.class.getCanonicalName()
									+ "' with key '"
									+ entity.getId()
									+ "'. Cause : InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}
		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		em.refresh(entity, ConsistencyLevel.ALL);
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_remove_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);

		try
		{
			em.remove(entity, ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		em.remove(entity, ConsistencyLevel.ALL);
		assertThat(em.find(CompleteBean.class, entity.getId())).isNull();
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_reinit_consistency_level_after_exception() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("name qzerferf")
				.buid();
		try
		{
			em.merge(entity, ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		em.merge(entity, ConsistencyLevel.ALL);
		CompleteBean found = em.find(CompleteBean.class, entity.getId());
		assertThat(found.getName()).isEqualTo("name qzerferf");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_insert_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<UUID, String> tweets = entity.getTweets();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		try
		{
			tweets.insert(uuid, "new tweet", ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}

		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		tweets.insert(uuid, "new tweet 5431", ConsistencyLevel.ALL);
		KeyValue<UUID, String> found = tweets.findFirst();
		assertThat(found.getKey()).isEqualTo(uuid);
		assertThat(found.getValue()).isEqualTo("new tweet 5431");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
	}

	@Test
	public void should_find_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<UUID, String> tweets = entity.getTweets();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		tweets.insert(uuid, "new tweet");
		try
		{
			tweets.findFirst(ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}

		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		KeyValue<UUID, String> found = tweets.findFirst(ConsistencyLevel.ALL);
		assertThat(found.getKey()).isEqualTo(uuid);
		assertThat(found.getValue()).isEqualTo("new tweet");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_find_value_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<UUID, String> tweets = entity.getTweets();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		tweets.insert(uuid, "new tweet erjkesdf");

		try
		{
			tweets.findFirstValue(ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}

		assertThatConsistencyLevelsAreReinitialized();

		logAsserter.prepareLogLevel();
		String found = tweets.findFirstValue(ConsistencyLevel.ALL);
		assertThat(found).isEqualTo("new tweet erjkesdf");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_find_key_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<UUID, String> tweets = entity.getTweets();

		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		tweets.insert(uuid, "new tweet");

		try
		{
			tweets.findFirstKey(ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}

		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		UUID found = tweets.findFirstKey(ConsistencyLevel.ALL);
		assertThat(found).isEqualTo(uuid);
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_iterate_on_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<UUID, String> tweets = entity.getTweets();

		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		tweets.insert(uuid, "new tweet etef");

		try
		{
			tweets.iterator(ConsistencyLevel.EACH_QUORUM).hasNext();
		}
		catch (AchillesException e)
		{
			assertThat(e)
					.hasMessage(
							"info.archinnov.achilles.exception.AchillesException: me.prettyprint.hector.api.exceptions.HInvalidRequestException: InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}

		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		KeyValue<UUID, String> found = tweets.iterator().next();
		assertThat(found.getKey()).isEqualTo(uuid);
		assertThat(found.getValue()).isEqualTo("new tweet etef");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_iterate_on_join_widemap_with_runtime_consistency_level() throws Exception
	{
		User user = UserTestBuilder.user().id(15431654L).firstname("fn").buid();
		user = em.merge(user);

		Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
		WideMap<Integer, Tweet> tweets = user.getTweets();
		tweets.insert(1, tweet);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("me.prettyprint.hector.api.exceptions.HInvalidRequestException: InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");

		tweets.iterator(ConsistencyLevel.EACH_QUORUM).hasNext();
	}

	@Test
	public void should_remove_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<UUID, String> tweets = entity.getTweets();

		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		tweets.insert(uuid, "new tweet");
		expectedEx.expect(HInvalidRequestException.class);
		expectedEx
				.expectMessage("InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

		tweets.remove(uuid, ConsistencyLevel.LOCAL_QUORUM);
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_batch_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		Tweet tweet = TweetTestBuilder.tweet().randomId().content("test_tweet").buid();

		logAsserter.prepareLogLevel();
		ThriftBatchingEntityManager batchingEm = em.batchingEntityManager();
		batchingEm.startBatch(ConsistencyLevel.ALL, ConsistencyLevel.ONE);
		batchingEm.persist(entity);
		batchingEm.persist(tweet);

		batchingEm.endBatch();
		logAsserter.assertConsistencyLevels(ALL, ONE);
	}

	@Test
	public void should_get_counter_with_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		try
		{
			entity.getVersion().get(ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_increment_counter_with_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		try
		{
			entity.getVersion().incr(ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_increment_n_counter_with_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		try
		{
			entity.getVersion().incr(10L, ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_decrement_counter_with_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		try
		{
			entity.getVersion().decr(ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_decrement_counter_n_with_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		try
		{
			entity.getVersion().decr(10L, ConsistencyLevel.EACH_QUORUM);
		}
		catch (HInvalidRequestException e)
		{
			assertThat(e)
					.hasMessage(
							"InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
		}
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_incr_with_consistency_level_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		logAsserter.prepareLogLevel();
		counterWideMap.insert(10, CounterBuilder.incr(ALL));
		logAsserter.assertConsistencyLevels(QUORUM, ALL);
		assertThatConsistencyLevelsAreReinitialized();

		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(1L);
	}

	@Test
	public void should_incr_n_with_consistency_level_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		logAsserter.prepareLogLevel();
		counterWideMap.insert(10, CounterBuilder.incr(15L, ALL));
		logAsserter.assertConsistencyLevels(QUORUM, ALL);
		assertThatConsistencyLevelsAreReinitialized();

		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(15L);
	}

	@Test
	public void should_decr_with_consistency_level_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		logAsserter.prepareLogLevel();
		counterWideMap.insert(10, CounterBuilder.decr(ALL));
		logAsserter.assertConsistencyLevels(QUORUM, ALL);
		assertThatConsistencyLevelsAreReinitialized();

		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(-1L);
	}

	@Test
	public void should_decr_n_with_consistency_level_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		logAsserter.prepareLogLevel();
		counterWideMap.insert(10, CounterBuilder.decr(15L, ALL));
		logAsserter.assertConsistencyLevels(QUORUM, ALL);
		assertThatConsistencyLevelsAreReinitialized();

		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(-15L);
	}

	private BeanWithConsistencyLevelOnClassAndField prepareCounterWideMap()
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);

		return entity;
	}

	private Composite prepareCounterWideMapName(Integer index)
	{
		Composite comp = new Composite();
		comp.addComponent(10, ThriftSerializerUtils.INT_SRZ);
		return comp;
	}

	private void assertThatConsistencyLevelsAreReinitialized()
	{
		assertThat(policy.getCurrentReadLevel()).isNull();
		assertThat(policy.getCurrentWriteLevel()).isNull();
	}

	@After
	public void cleanThreadLocals()
	{
		policy.reinitCurrentConsistencyLevels();
		policy.reinitDefaultConsistencyLevels();
		cluster.truncate(keyspaceName, "CompleteBean");
		cluster.truncate(keyspaceName, "Tweet");
	}

	@AfterClass
	public static void cleanUp()
	{
		ThriftCassandraDaoTest.getConsistencyPolicy().reinitCurrentConsistencyLevels();
		ThriftCassandraDaoTest.getConsistencyPolicy().reinitDefaultConsistencyLevels();
	}
}
