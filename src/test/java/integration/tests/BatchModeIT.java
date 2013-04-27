package integration.tests;

import static info.archinnov.achilles.columnFamily.ThriftColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.ThriftCassandraDaoTest.*;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.context.BatchingFlushContext;
import info.archinnov.achilles.entity.manager.ThriftBatchingEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.serializer.SerializerUtils;
import info.archinnov.achilles.wrapper.CounterBuilder;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;
import integration.tests.utils.CassandraLogAsserter;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

/**
 * BatchModeIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class BatchModeIT
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private GenericEntityDao<UUID> tweetDao = getEntityDao(SerializerUtils.UUID_SRZ,
			normalizerAndValidateColumnFamilyName(Tweet.class.getCanonicalName()));

	private GenericEntityDao<Long> userDao = getEntityDao(SerializerUtils.LONG_SRZ,
			normalizerAndValidateColumnFamilyName(User.class.getCanonicalName()));

	private GenericColumnFamilyDao<Long, UUID> userTweetsDao = getColumnFamilyDao(LONG_SRZ,
			SerializerUtils.UUID_SRZ, "user_tweets");

	private GenericColumnFamilyDao<Long, Long> popularTopicsDao = ThriftCassandraDaoTest
			.getColumnFamilyDao(LONG_SRZ, LONG_SRZ, "complete_bean_popular_topics");

	private GenericEntityDao<Long> completeBeanDao = getEntityDao(SerializerUtils.LONG_SRZ,
			normalizerAndValidateColumnFamilyName(CompleteBean.class.getCanonicalName()));

	private GenericColumnFamilyDao<Long, String> externalWideMapDao = getColumnFamilyDao(LONG_SRZ,
			STRING_SRZ, "ExternalWideMap");

	private CounterDao counterDao = getCounterDao();

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private AchillesConfigurableConsistencyLevelPolicy policy = ThriftCassandraDaoTest
			.getConsistencyPolicy();

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private Tweet ownTweet1;
	private Tweet ownTweet2;

	private User user;

	private Long userId = RandomUtils.nextLong();

	@Before
	public void setUp()
	{
		user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();
		ownTweet1 = TweetTestBuilder.tweet().randomId().content("myTweet1").buid();
		ownTweet2 = TweetTestBuilder.tweet().randomId().content("myTweet2").buid();
	}

	@Test
	public void should_batch_join_wide_map() throws Exception
	{
		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		user = batchEm.merge(user);

		Composite startComp = new Composite();
		startComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 2, ComponentEquality.GREATER_THAN_EQUAL);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);

		List<Pair<Composite, UUID>> columns = userTweetsDao.findColumnsRange(user.getId(),
				startComp, endComp, false, 20);

		Tweet foundOwnTweet1 = batchEm.find(Tweet.class, ownTweet1.getId());
		Tweet foundOwnTweet2 = batchEm.find(Tweet.class, ownTweet2.getId());

		assertThat(columns).isEmpty();
		assertThat(foundOwnTweet1).isNull();
		assertThat(foundOwnTweet2).isNull();

		// End batch
		batchEm.endBatch();

		columns = userTweetsDao.findColumnsRange(user.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);

		assertThat(columns.get(0).right).isEqualTo(ownTweet1.getId());
		assertThat(columns.get(1).right).isEqualTo(ownTweet2.getId());

		foundOwnTweet1 = batchEm.find(Tweet.class, ownTweet1.getId());
		foundOwnTweet2 = batchEm.find(Tweet.class, ownTweet2.getId());

		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet2.getContent());
		assertThatBatchContextHasBeenReset(batchEm);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_batch_external_widemap_simple_join_and_counters() throws Exception
	{
		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

		entity = batchEm.merge(entity);

		entity.setLabel("label");

		entity.getExternalWideMap().insert(1, "one");
		entity.getExternalWideMap().insert(2, "two");

		Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("welcomeTweet").buid();
		entity.setWelcomeTweet(welcomeTweet);

		entity.getVersion().incr(10L);
		entity.getPopularTopics().insert("java", CounterBuilder.incr(100L));
		entity.getPopularTopics().insert("scala", CounterBuilder.incr(35L));
		batchEm.merge(entity);

		Composite labelComposite = new Composite();
		labelComposite.addComponent(0, LAZY_SIMPLE.flag(), EQUAL);
		labelComposite.addComponent(1, "label", EQUAL);
		labelComposite.addComponent(2, 0, EQUAL);

		assertThat(completeBeanDao.getValue(entity.getId(), labelComposite)).isNull();

		Composite startWideMapComp = new Composite();
		startWideMapComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endWideMapComp = new Composite();
		endWideMapComp.addComponent(0, 5, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = externalWideMapDao.findColumnsRange(entity.getId(),
				startWideMapComp, endWideMapComp, false, 20);
		assertThat(columns).isEmpty();

		Tweet foundTweet = batchEm.find(Tweet.class, welcomeTweet.getId());
		assertThat(foundTweet).isNull();

		Composite counterKey = createCounterKey(CompleteBean.class, entity.getId());
		Composite versionCounterName = createCounterName("version");

		Composite javaCounterName = createWideMapCounterName("java");
		Composite scalaCounterName = createWideMapCounterName("scala");

		assertThat(counterDao.getCounterValue(counterKey, versionCounterName)).isEqualTo(10L);
		assertThat(popularTopicsDao.getCounterValue(entity.getId(), javaCounterName)).isEqualTo(
				100L);
		assertThat(popularTopicsDao.getCounterValue(entity.getId(), scalaCounterName)).isEqualTo(
				35L);

		// Flush
		batchEm.endBatch();

		assertThat(completeBeanDao.getValue(entity.getId(), labelComposite)).isEqualTo("label");

		columns = externalWideMapDao.findColumnsRange(entity.getId(), startWideMapComp,
				endWideMapComp, false, 20);
		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.getComponent(0).getValue(INT_SRZ)).isEqualTo(1);
		assertThat(columns.get(0).right).isEqualTo("one");
		assertThat(columns.get(1).left.getComponent(0).getValue(INT_SRZ)).isEqualTo(2);
		assertThat(columns.get(1).right).isEqualTo("two");

		foundTweet = batchEm.find(Tweet.class, welcomeTweet.getId());
		assertThat(foundTweet.getId()).isEqualTo(welcomeTweet.getId());
		assertThat(foundTweet.getContent()).isEqualTo("welcomeTweet");

		assertThat(counterDao.getCounterValue(counterKey, versionCounterName)).isEqualTo(10L);
		assertThat(popularTopicsDao.getCounterValue(entity.getId(), javaCounterName)).isEqualTo(
				100L);
		assertThat(popularTopicsDao.getCounterValue(entity.getId(), scalaCounterName)).isEqualTo(
				35L);
		assertThatBatchContextHasBeenReset(batchEm);
	}

	@Test
	public void should_batch_external_join_widemap() throws Exception
	{
		Tweet reTweet1 = TweetTestBuilder.tweet().randomId().content("reTweet1").buid();
		Tweet reTweet2 = TweetTestBuilder.tweet().randomId().content("reTweet2").buid();

		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		user = batchEm.merge(user);
		WideMap<Integer, Tweet> retweets = user.getRetweets();

		retweets.insert(1, reTweet1);
		retweets.insert(2, reTweet2);

		Tweet foundRetweet1 = batchEm.find(Tweet.class, reTweet1.getId());
		Tweet foundRetweet2 = batchEm.find(Tweet.class, reTweet2.getId());

		assertThat(foundRetweet1).isNull();
		assertThat(foundRetweet2).isNull();

		// Flush
		batchEm.endBatch();

		foundRetweet1 = batchEm.find(Tweet.class, reTweet1.getId());
		foundRetweet2 = batchEm.find(Tweet.class, reTweet2.getId());

		assertThat(foundRetweet1.getContent()).isEqualTo("reTweet1");
		assertThat(foundRetweet2.getContent()).isEqualTo("reTweet2");
		assertThatBatchContextHasBeenReset(batchEm);
	}

	@Test
	public void should_batch_several_entities() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();

		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		batchEm.merge(bean);
		batchEm.merge(tweet1);
		batchEm.merge(tweet2);
		batchEm.merge(user);

		CompleteBean foundBean = batchEm.find(CompleteBean.class, bean.getId());
		Tweet foundTweet1 = batchEm.find(Tweet.class, tweet1.getId());
		Tweet foundTweet2 = batchEm.find(Tweet.class, tweet2.getId());
		User foundUser = batchEm.find(User.class, user.getId());

		assertThat(foundBean).isNull();
		assertThat(foundTweet1).isNull();
		assertThat(foundTweet2).isNull();
		assertThat(foundUser).isNull();

		// Flush
		batchEm.endBatch();

		foundBean = batchEm.find(CompleteBean.class, bean.getId());
		foundTweet1 = batchEm.find(Tweet.class, tweet1.getId());
		foundTweet2 = batchEm.find(Tweet.class, tweet2.getId());
		foundUser = batchEm.find(User.class, user.getId());

		assertThat(foundBean.getName()).isEqualTo("name");
		assertThat(foundTweet1.getContent()).isEqualTo("tweet1");
		assertThat(foundTweet2.getContent()).isEqualTo("tweet2");
		assertThat(foundUser.getFirstname()).isEqualTo("fn");
		assertThat(foundUser.getLastname()).isEqualTo("ln");
		assertThatBatchContextHasBeenReset(batchEm);
		assertThatConsistencyLevelHasBeenReset();
	}

	@Test
	public void should_reinit_batch_context_after_exception() throws Exception
	{
		User user = UserTestBuilder.user().id(123456494L).firstname("firstname")
				.lastname("lastname").buid();
		Tweet tweet = TweetTestBuilder.tweet().randomId().content("simple_tweet").creator(user)
				.buid();

		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		try
		{
			batchEm.persist(tweet);
		}
		catch (AchillesException e)
		{
			batchEm.cleanBatch();
			assertThatBatchContextHasBeenReset(batchEm);
			assertThatConsistencyLevelHasBeenReset();

			assertThat(batchEm.find(Tweet.class, tweet.getId())).isNull();
		}

		// batchEm should reinit batch context
		batchEm.persist(user);
		batchEm.endBatch();

		User foundUser = batchEm.find(User.class, user.getId());
		assertThat(foundUser.getFirstname()).isEqualTo("firstname");
		assertThat(foundUser.getLastname()).isEqualTo("lastname");

		batchEm.persist(tweet);
		batchEm.endBatch();

		Tweet foundTweet = batchEm.find(Tweet.class, tweet.getId());
		assertThat(foundTweet.getContent()).isEqualTo("simple_tweet");
		assertThat(foundTweet.getCreator().getId()).isEqualTo(foundUser.getId());
		assertThat(foundTweet.getCreator().getFirstname()).isEqualTo("firstname");
		assertThat(foundTweet.getCreator().getLastname()).isEqualTo("lastname");
		assertThatBatchContextHasBeenReset(batchEm);
	}

	@Test
	public void should_batch_with_custom_consistency_level() throws Exception
	{
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();
		Tweet tweet3 = TweetTestBuilder.tweet().randomId().content("simple_tweet3").buid();

		em.persist(tweet1);

		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		batchEm.startBatch(ONE, ALL);

		logAsserter.prepareLogLevel();

		Tweet foundTweet1 = batchEm.find(Tweet.class, tweet1.getId());

		assertThat(foundTweet1.getContent()).isEqualTo(tweet1.getContent());

		batchEm.persist(tweet2);
		batchEm.persist(tweet3);

		batchEm.endBatch();

		logAsserter.assertConsistencyLevels(ONE, ALL);
		assertThatBatchContextHasBeenReset(batchEm);
		assertThatConsistencyLevelHasBeenReset();
	}

	@Test
	public void should_reinit_batch_context_and_consistency_after_exception() throws Exception
	{
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();

		em.persist(tweet1);

		// Start batch
		ThriftBatchingEntityManager batchEm = em.batchingEntityManager();
		batchEm.startBatch();

		batchEm.startBatch(EACH_QUORUM, EACH_QUORUM);
		batchEm.persist(tweet2);

		try
		{
			batchEm.endBatch();
		}
		catch (Exception e)
		{
			assertThatBatchContextHasBeenReset(batchEm);
			assertThatConsistencyLevelHasBeenReset();
		}

		Thread.sleep(1000);
		logAsserter.prepareLogLevel();
		batchEm.persist(tweet2);

		logAsserter.assertConsistencyLevels(QUORUM, QUORUM);
	}

	private void assertThatConsistencyLevelHasBeenReset()
	{
		assertThat(policy.getCurrentReadLevel()).isNull();
		assertThat(policy.getCurrentWriteLevel()).isNull();
	}

	private void assertThatBatchContextHasBeenReset(ThriftBatchingEntityManager batchEm)
	{
		BatchingFlushContext flushContext = Whitebox.getInternalState(batchEm, "flushContext");
		Map<String, Pair<Mutator<?>, AbstractDao<?, ?>>> mutatorMap = Whitebox.getInternalState(
				flushContext, "mutatorMap");
		boolean hasCustomConsistencyLevels = (Boolean) Whitebox.getInternalState(flushContext,
				"hasCustomConsistencyLevels");

		assertThat(mutatorMap).isEmpty();
		assertThat(hasCustomConsistencyLevels).isFalse();

	}

	private <T> Composite createCounterKey(Class<T> clazz, Long id)
	{
		Composite comp = new Composite();
		comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
		comp.setComponent(1, id.toString(), STRING_SRZ);
		return comp;
	}

	private Composite createCounterName(String propertyName)
	{
		Composite composite = new Composite();
		composite.addComponent(0, propertyName, ComponentEquality.EQUAL);
		return composite;
	}

	private Composite createWideMapCounterName(String key)
	{
		Composite composite = new Composite();
		composite.addComponent(0, key, ComponentEquality.EQUAL);
		return composite;
	}

	@After
	public void tearDown()
	{
		tweetDao.truncate();
		userDao.truncate();
		completeBeanDao.truncate();
	}

	@AfterClass
	public static void cleanUp()
	{
		ThriftCassandraDaoTest.getConsistencyPolicy().reinitCurrentConsistencyLevels();
		ThriftCassandraDaoTest.getConsistencyPolicy().reinitDefaultConsistencyLevels();
	}
}
