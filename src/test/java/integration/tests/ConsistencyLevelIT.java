package integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
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
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;
import integration.tests.utils.CassandraLogAsserter;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
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

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private Cluster cluster = CassandraDaoTest.getCluster();

	private String keyspaceName = CassandraDaoTest.getKeyspace().getKeyspaceName();

	private Long id = RandomUtils.nextLong();

	private AchillesConfigurableConsistencyLevelPolicy policy = CassandraDaoTest
			.getConsistencyPolicy();

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
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_exception_when_writing_to_widemap_with_local_quorum_consistency()
			throws Exception
	{
		BeanWithWriteLocalQuorumConsistencyForExternalWidemap bean = new BeanWithWriteLocalQuorumConsistencyForExternalWidemap(
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
		BeanWithReadLocalQuorumConsistencyForExternalWidemap bean = new BeanWithReadLocalQuorumConsistencyForExternalWidemap(
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
		BeanWithReadOneWriteAllConsistencyForExternalWidemap newBean = new BeanWithReadOneWriteAllConsistencyForExternalWidemap(
				id, "name");

		em.persist(newBean);

		newBean = em.find(BeanWithReadOneWriteAllConsistencyForExternalWidemap.class,
				newBean.getId());

		assertThat(newBean).isNotNull();
		assertThat(newBean.getName()).isEqualTo("name");
		assertThatConsistencyLevelsAreReinitialized();
	}

	@Test
	public void should_persist_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name zerferg")
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
		logAsserter.assertConsistencyLevels(ConsistencyLevel.QUORUM, ConsistencyLevel.ALL);
	}

	@Test
	public void should_merge_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name zeruioze")
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
		logAsserter.assertConsistencyLevels(ConsistencyLevel.QUORUM, ConsistencyLevel.ALL);
	}

	@Test
	public void should_find_with_runtime_consistency_level_overriding_predefined_one()
			throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name rtprt")
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
									+ entity.getClass().getCanonicalName()
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
		logAsserter.assertConsistencyLevels(ConsistencyLevel.QUORUM, ConsistencyLevel.ALL);
	}

	@Test
	public void should_reinit_consistency_level_after_exception() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name qzerferf")
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
		logAsserter.assertConsistencyLevels(ConsistencyLevel.QUORUM, ConsistencyLevel.ALL);
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
		logAsserter.assertConsistencyLevels(ConsistencyLevel.QUORUM, ConsistencyLevel.ALL);
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
							"me.prettyprint.hector.api.exceptions.HInvalidRequestException: InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}

		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		KeyValue<UUID, String> found = tweets.iterator().next();
		assertThat(found.getKey()).isEqualTo(uuid);
		assertThat(found.getValue()).isEqualTo("new tweet etef");
		logAsserter.assertConsistencyLevels(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_iterate_on_counter_widemap_with_runtime_consistency_level() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		entity = em.merge(entity);
		WideMap<String, Long> popularTopics = entity.getPopularTopics();

		popularTopics.insert("java", 110L);
		try
		{
			popularTopics.iterator(ConsistencyLevel.EACH_QUORUM).hasNext();
		}
		catch (AchillesException e)
		{
			assertThat(e)
					.hasMessage(
							"me.prettyprint.hector.api.exceptions.HInvalidRequestException: InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
		}
		assertThatConsistencyLevelsAreReinitialized();
		logAsserter.prepareLogLevel();
		KeyValue<String, Long> found = popularTopics.iterator(ConsistencyLevel.ALL).next();
		assertThat(found.getKey()).isEqualTo("java");
		assertThat(found.getValue()).isEqualTo(110L);
		logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
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
		assertThatConsistencyLevelsAreReinitialized();
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

		em.startBatch(ConsistencyLevel.ALL, ConsistencyLevel.ONE);

		em.persist(entity);
		em.persist(tweet);

		Logger thriftLogger = Logger.getLogger("org.apache.cassandra.service.StorageProxy");
		thriftLogger.setLevel(Level.TRACE);

		ConsoleAppender ca = new ConsoleAppender();
		final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
		ca.setWriter(new OutputStreamWriter(myOut));
		ca.setLayout(new PatternLayout("%-5p [%d{ABSOLUTE}][%x] %c@:%M %m %n"));
		ca.setName("test appender");
		thriftLogger.addAppender(ca);

		em.endBatch();
		final String standardOutput = myOut.toString();

		System.out.println(" : " + standardOutput);
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
		CassandraDaoTest.getConsistencyPolicy().reinitCurrentConsistencyLevels();
		CassandraDaoTest.getConsistencyPolicy().reinitDefaultConsistencyLevels();
	}
}
