package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getDynamicCompositeDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.serializer.SerializerUtils;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;

import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * JoinWideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapIT
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private GenericDynamicCompositeDao<UUID> tweetDao = getDynamicCompositeDao(
			SerializerUtils.UUID_SRZ,
			normalizerAndValidateColumnFamilyName(Tweet.class.getCanonicalName()));

	private GenericDynamicCompositeDao<Long> userDao = getDynamicCompositeDao(
			SerializerUtils.LONG_SRZ,
			normalizerAndValidateColumnFamilyName(User.class.getCanonicalName()));

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private Tweet ownTweet1;
	private Tweet ownTweet2;
	private Tweet ownTweet3;
	private Tweet ownTweet4;

	private User user;

	private Long userId = RandomUtils.nextLong();

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();

		ownTweet1 = TweetTestBuilder.tweet().randomId().content("myTweet1").creator(user).buid();
		ownTweet2 = TweetTestBuilder.tweet().randomId().content("myTweet2").creator(user).buid();
		ownTweet3 = TweetTestBuilder.tweet().randomId().content("myTweet3").creator(user).buid();
		ownTweet4 = TweetTestBuilder.tweet().randomId().content("myTweet4").creator(user).buid();

	}

	@Test
	public void should_insert_join_tweets() throws Exception
	{

		user = em.merge(user);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);

		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "tweets", ComponentEquality.EQUAL);

		DynamicComposite endComp = new DynamicComposite();
		endComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
		endComp.addComponent(1, "tweets", ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, String>> columns = userDao.findColumnsRange(user.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);

		assertThat(readUUID(columns.get(0).right)).isEqualTo(ownTweet1.getId());
		assertThat(readUUID(columns.get(1).right)).isEqualTo(ownTweet2.getId());

		Tweet foundOwnTweet1 = em.find(Tweet.class, ownTweet1.getId());
		Tweet foundOwnTweet2 = em.find(Tweet.class, ownTweet2.getId());

		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet2.getContent());
	}

	@Test
	public void should_find_join_tweets() throws Exception
	{

		user = em.merge(user);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);

		List<KeyValue<Integer, Tweet>> foundOwnTweetsKeyValues = user.getTweets().findReverse(2, 1,
				5);

		assertThat(foundOwnTweetsKeyValues).hasSize(2);

		Tweet foundOwnTweet1 = foundOwnTweetsKeyValues.get(0).getValue();
		Tweet foundOwnTweet2 = foundOwnTweetsKeyValues.get(1).getValue();

		assertThat(foundOwnTweetsKeyValues.get(0).getKey()).isEqualTo(2);
		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet2.getContent());
		assertThat(foundOwnTweetsKeyValues.get(1).getKey()).isEqualTo(1);
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet1.getContent());

		List<Tweet> foundOwnTweetsValues = user.getTweets().findReverseValues(2, 1, 5);

		assertThat(foundOwnTweetsValues.get(0).getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweetsValues.get(0).getContent()).isEqualTo(ownTweet2.getContent());
		assertThat(foundOwnTweetsValues.get(1).getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweetsValues.get(1).getContent()).isEqualTo(ownTweet1.getContent());

		List<Integer> foundOwnTweetsKeys = user.getTweets().findReverseKeys(2, 1, 5);

		assertThat(foundOwnTweetsKeys.get(0)).isEqualTo(2);
		assertThat(foundOwnTweetsKeys.get(1)).isEqualTo(1);
	}

	@Test
	public void should_remove_join_tweets() throws Exception
	{

		user = em.merge(user);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);
		user.getTweets().insert(3, ownTweet3);
		user.getTweets().insert(4, ownTweet4);

		user.getTweets().remove(2, 4, BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		List<KeyValue<Integer, Tweet>> foundOwnTweets = user.getTweets().find(1, 4, 10);

		assertThat(foundOwnTweets).hasSize(2);

		Tweet foundOwnTweet1 = foundOwnTweets.get(0).getValue();
		Tweet foundOwnTweet2 = foundOwnTweets.get(1).getValue();

		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet4.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet4.getContent());

		assertThat(em.find(Tweet.class, ownTweet1.getId())).isNotNull();
		assertThat(em.find(Tweet.class, ownTweet2.getId())).isNotNull();
		assertThat(em.find(Tweet.class, ownTweet3.getId())).isNotNull();
		assertThat(em.find(Tweet.class, ownTweet4.getId())).isNotNull();
	}

	@Test
	public void should_iterate_through_tweets() throws Exception
	{
		user = em.merge(user);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);
		user.getTweets().insert(3, ownTweet3);
		user.getTweets().insert(4, ownTweet4);

		KeyValueIterator<Integer, Tweet> iterator = user.getTweets().iterator(1, 3, 10, BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING);

		Tweet foundOwnTweet1 = iterator.next().getValue();
		Tweet foundOwnTweet2 = iterator.next().getValue();

		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet2.getContent());
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet3.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet3.getContent());

	}

	@Test
	public void should_exception_when_inserting_tweets_which_do_not_exist_in_db() throws Exception
	{
		user = em.merge(user);

		Tweet unkonwTweet = TweetTestBuilder.tweet().randomId().content("unkonwTweet")
				.creator(user).buid();

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The entity '"
						+ Tweet.class.getCanonicalName()
						+ "' with id '"
						+ unkonwTweet.getId().toString()
						+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST");

		user.getTimeline().insert(RandomUtils.nextLong(), unkonwTweet);
	}

	@Test
	public void should_proxy_join_entity() throws Exception
	{
		user = em.merge(user);
		user.getTweets().insert(1, ownTweet1);

		Tweet tweetProxy = user.getTweets().get(1);
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getTweets().findFirst().getValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getTweets().findFirstValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getTweets().iterator(null, null, 1).next().getValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getTweets().iterator(null, null, 1).nextValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);
	}

	private UUID readUUID(String value) throws Exception
	{
		return this.objectMapper.readValue(value, UUID.class);
	}

	@After
	public void tearDown()
	{
		tweetDao.truncate();
	}
}
