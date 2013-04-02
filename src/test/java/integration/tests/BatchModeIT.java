package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getCompositeDao;
import static info.archinnov.achilles.common.CassandraDaoTest.getCounterDao;
import static info.archinnov.achilles.common.CassandraDaoTest.getDynamicCompositeDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.COUNTER;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP_COUNTER;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.QUORUM;
import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.context.FlushContext;
import info.archinnov.achilles.entity.context.FlushContext.BatchType;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.serializer.SerializerUtils;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;
import integration.tests.utils.CassandraLogAsserter;

import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.util.reflection.Whitebox;

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

	private GenericEntityDao<UUID> tweetDao = getDynamicCompositeDao(SerializerUtils.UUID_SRZ,
			normalizerAndValidateColumnFamilyName(Tweet.class.getCanonicalName()));

	private GenericEntityDao<Long> userDao = getDynamicCompositeDao(SerializerUtils.LONG_SRZ,
			normalizerAndValidateColumnFamilyName(User.class.getCanonicalName()));

	private GenericEntityDao<Long> completeBeanDao = getDynamicCompositeDao(
			SerializerUtils.LONG_SRZ,
			normalizerAndValidateColumnFamilyName(CompleteBean.class.getCanonicalName()));

	private GenericColumnFamilyDao<Long, String> externalWideMapDao = getCompositeDao(LONG_SRZ,
			STRING_SRZ, "ExternalWideMap");

	private CounterDao counterDao = getCounterDao();

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private AchillesConfigurableConsistencyLevelPolicy policy = CassandraDaoTest
			.getConsistencyPolicy();

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private Tweet ownTweet1;
	private Tweet ownTweet2;

	private User user;

	private Long userId = RandomUtils.nextLong();

	private ObjectMapper objectMapper = new ObjectMapper();

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
		em.startBatch();

		user = em.merge(user);

		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "tweets", ComponentEquality.EQUAL);

		DynamicComposite endComp = new DynamicComposite();
		endComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
		endComp.addComponent(1, "tweets", ComponentEquality.GREATER_THAN_EQUAL);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);

		List<Pair<DynamicComposite, String>> columns = userDao.findColumnsRange(user.getId(),
				startComp, endComp, false, 20);

		Tweet foundOwnTweet1 = em.find(Tweet.class, ownTweet1.getId());
		Tweet foundOwnTweet2 = em.find(Tweet.class, ownTweet2.getId());

		assertThat(columns).isEmpty();
		assertThat(foundOwnTweet1).isNull();
		assertThat(foundOwnTweet2).isNull();

		// End batch
		em.endBatch();

		columns = userDao.findColumnsRange(user.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);

		assertThat(readUUID(columns.get(0).right)).isEqualTo(ownTweet1.getId());
		assertThat(readUUID(columns.get(1).right)).isEqualTo(ownTweet2.getId());

		foundOwnTweet1 = em.find(Tweet.class, ownTweet1.getId());
		foundOwnTweet2 = em.find(Tweet.class, ownTweet2.getId());

		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet2.getContent());
		assertThatBatchContextHasBeenReset();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_batch_external_widemap_simple_join_and_counters() throws Exception
	{
		// Start batch
		em.startBatch();

		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

		entity = em.merge(entity);

		entity.setLabel("label");

		entity.getExternalWideMap().insert(1, "one");
		entity.getExternalWideMap().insert(2, "two");

		Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("welcomeTweet").buid();
		entity.setWelcomeTweet(welcomeTweet);

		entity.setVersion(10);
		entity.getPopularTopics().insert("java", 100L);
		entity.getPopularTopics().insert("scala", 35L);
		em.merge(entity);

		DynamicComposite labelComposite = new DynamicComposite();
		labelComposite.addComponent(0, LAZY_SIMPLE.flag(), EQUAL);
		labelComposite.addComponent(1, "label", EQUAL);

		assertThat(completeBeanDao.getValue(entity.getId(), labelComposite)).isNull();

		Composite startWideMapComp = new Composite();
		startWideMapComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endWideMapComp = new Composite();
		endWideMapComp.addComponent(0, 5, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = externalWideMapDao.findColumnsRange(entity.getId(),
				startWideMapComp, endWideMapComp, false, 20);
		assertThat(columns).isEmpty();

		Tweet foundTweet = em.find(Tweet.class, welcomeTweet.getId());
		assertThat(foundTweet).isNull();

		Composite counterKey = createCounterKey(CompleteBean.class, entity.getId());
		DynamicComposite versionCounterName = createCounterName(COUNTER, "version");

		DynamicComposite javaCounterName = createCounterName(WIDE_MAP_COUNTER, "popularTopics",
				"java");
		DynamicComposite scalaCounterName = createCounterName(WIDE_MAP_COUNTER, "popularTopics",
				"scala");

		assertThat(counterDao.getCounterValue(counterKey, versionCounterName)).isEqualTo(0L);
		assertThat(counterDao.getCounterValue(counterKey, javaCounterName)).isEqualTo(0L);
		assertThat(counterDao.getCounterValue(counterKey, scalaCounterName)).isEqualTo(0L);

		// Flush
		em.endBatch();

		assertThat(completeBeanDao.getValue(entity.getId(), labelComposite)).isEqualTo("label");

		columns = externalWideMapDao.findColumnsRange(entity.getId(), startWideMapComp,
				endWideMapComp, false, 20);
		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.getComponent(0).getValue(INT_SRZ)).isEqualTo(1);
		assertThat(columns.get(0).right).isEqualTo("one");
		assertThat(columns.get(1).left.getComponent(0).getValue(INT_SRZ)).isEqualTo(2);
		assertThat(columns.get(1).right).isEqualTo("two");

		foundTweet = em.find(Tweet.class, welcomeTweet.getId());
		assertThat(foundTweet.getId()).isEqualTo(welcomeTweet.getId());
		assertThat(foundTweet.getContent()).isEqualTo("welcomeTweet");

		assertThat(counterDao.getCounterValue(counterKey, versionCounterName)).isEqualTo(10L);
		assertThat(counterDao.getCounterValue(counterKey, javaCounterName)).isEqualTo(100L);
		assertThat(counterDao.getCounterValue(counterKey, scalaCounterName)).isEqualTo(35L);
		assertThatBatchContextHasBeenReset();
	}

	@Test
	public void should_batch_external_join_widemap() throws Exception
	{
		Tweet reTweet1 = TweetTestBuilder.tweet().randomId().content("reTweet1").buid();
		Tweet reTweet2 = TweetTestBuilder.tweet().randomId().content("reTweet2").buid();

		// Start batch
		em.startBatch();

		user = em.merge(user);
		WideMap<Integer, Tweet> retweets = user.getRetweets();

		retweets.insert(1, reTweet1);
		retweets.insert(2, reTweet2);

		Tweet foundRetweet1 = em.find(Tweet.class, reTweet1.getId());
		Tweet foundRetweet2 = em.find(Tweet.class, reTweet2.getId());

		assertThat(foundRetweet1).isNull();
		assertThat(foundRetweet2).isNull();

		// Flush
		em.endBatch();

		foundRetweet1 = em.find(Tweet.class, reTweet1.getId());
		foundRetweet2 = em.find(Tweet.class, reTweet2.getId());

		assertThat(foundRetweet1.getContent()).isEqualTo("reTweet1");
		assertThat(foundRetweet2.getContent()).isEqualTo("reTweet2");
		assertThatBatchContextHasBeenReset();
	}

	@Test
	public void should_batch_several_entities() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();

		// Start batch
		em.startBatch();

		em.merge(bean);
		em.merge(tweet1);
		em.merge(tweet2);
		em.merge(user);

		CompleteBean foundBean = em.find(CompleteBean.class, bean.getId());
		Tweet foundTweet1 = em.find(Tweet.class, tweet1.getId());
		Tweet foundTweet2 = em.find(Tweet.class, tweet2.getId());
		User foundUser = em.find(User.class, user.getId());

		assertThat(foundBean).isNull();
		assertThat(foundTweet1).isNull();
		assertThat(foundTweet2).isNull();
		assertThat(foundUser).isNull();

		// Flush
		em.endBatch();

		foundBean = em.find(CompleteBean.class, bean.getId());
		foundTweet1 = em.find(Tweet.class, tweet1.getId());
		foundTweet2 = em.find(Tweet.class, tweet2.getId());
		foundUser = em.find(User.class, user.getId());

		assertThat(foundBean.getName()).isEqualTo("name");
		assertThat(foundTweet1.getContent()).isEqualTo("tweet1");
		assertThat(foundTweet2.getContent()).isEqualTo("tweet2");
		assertThat(foundUser.getFirstname()).isEqualTo("fn");
		assertThat(foundUser.getLastname()).isEqualTo("ln");
		assertThatBatchContextHasBeenReset();
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
		em.startBatch();

		try
		{
			em.persist(tweet);
		}
		catch (AchillesException e)
		{
			assertThatBatchContextHasBeenReset();
			assertThatConsistencyLevelHasBeenReset();
		}

		// em should reinit batch context
		em.persist(user);

		User foundUser = em.find(User.class, user.getId());
		assertThat(foundUser.getFirstname()).isEqualTo("firstname");
		assertThat(foundUser.getLastname()).isEqualTo("lastname");

		em.persist(tweet);

		Tweet foundTweet = em.find(Tweet.class, tweet.getId());
		assertThat(foundTweet.getContent()).isEqualTo("simple_tweet");
		assertThat(foundTweet.getCreator().getId()).isEqualTo(foundUser.getId());
		assertThat(foundTweet.getCreator().getFirstname()).isEqualTo("firstname");
		assertThat(foundTweet.getCreator().getLastname()).isEqualTo("lastname");

	}

	@Test
	public void should_batch_with_custom_consistency_level() throws Exception
	{
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();
		Tweet tweet3 = TweetTestBuilder.tweet().randomId().content("simple_tweet3").buid();

		em.persist(tweet1);

		em.startBatch(ONE, ALL);

		logAsserter.prepareLogLevel();

		Tweet foundTweet1 = em.find(Tweet.class, tweet1.getId());

		assertThat(foundTweet1.getContent()).isEqualTo(tweet1.getContent());

		em.persist(tweet2);
		em.persist(tweet3);

		em.endBatch();

		logAsserter.assertConsistencyLevels(ONE, ALL);
		assertThatBatchContextHasBeenReset();
		assertThatConsistencyLevelHasBeenReset();
	}

	@Test
	public void should_reinit_batch_context_and_consistency_after_exception() throws Exception
	{
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();

		em.persist(tweet1);

		em.startBatch(EACH_QUORUM, EACH_QUORUM);
		em.persist(tweet2);

		try
		{
			em.endBatch();
		}
		catch (Exception e)
		{
			assertThatBatchContextHasBeenReset();
			assertThatConsistencyLevelHasBeenReset();
		}

		logAsserter.prepareLogLevel();
		em.persist(tweet2);

		logAsserter.assertConsistencyLevels(QUORUM, QUORUM);
	}

	private void assertThatBatchContextHasBeenReset()
	{
		assertThat(((FlushContext) Whitebox.getInternalState(em, "flushContext")).type())
				.isEqualTo(BatchType.NONE);
	}

	private void assertThatConsistencyLevelHasBeenReset()
	{
		assertThat(policy.getCurrentReadLevel()).isNull();
		assertThat(policy.getCurrentWriteLevel()).isNull();
	}

	private UUID readUUID(String value) throws Exception
	{
		return this.objectMapper.readValue(value, UUID.class);
	}

	private <T> Composite createCounterKey(Class<T> clazz, Long id)
	{
		Composite comp = new Composite();
		comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
		comp.setComponent(1, id.toString(), STRING_SRZ);
		return comp;
	}

	private DynamicComposite createCounterName(PropertyType type, String propertyName)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		return composite;
	}

	private DynamicComposite createCounterName(PropertyType type, String propertyName, String key)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		composite.addComponent(2, key, ComponentEquality.EQUAL);
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
		CassandraDaoTest.getConsistencyPolicy().reinitCurrentConsistencyLevels();
		CassandraDaoTest.getConsistencyPolicy().reinitDefaultConsistencyLevels();
	}
}
