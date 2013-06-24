package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.*;
import static info.archinnov.achilles.table.TableHelper.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;

import java.util.List;
import java.util.UUID;

import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * ExternalJoinWideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinWideMapIT
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private ThriftGenericEntityDao tweetDao = getEntityDao(
			normalizerAndValidateColumnFamilyName(Tweet.class.getCanonicalName()), UUID.class);

	private ThriftGenericWideRowDao externalJoinWideMapDao = getColumnFamilyDao(
			normalizerAndValidateColumnFamilyName("retweets_cf"), Long.class, UUID.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private Tweet reTweet1;
	private Tweet reTweet2;
	private Tweet reTweet3;
	private Tweet reTweet4;

	private User user;

	private Long userId = RandomUtils.nextLong();

	@Before
	public void setUp()
	{
		user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();

		reTweet1 = TweetTestBuilder.tweet().randomId().content("reTweet1").creator(user).buid();
		reTweet2 = TweetTestBuilder.tweet().randomId().content("reTweet2").creator(user).buid();
		reTweet3 = TweetTestBuilder.tweet().randomId().content("reTweet3").creator(user).buid();
		reTweet4 = TweetTestBuilder.tweet().randomId().content("reTweet4").creator(user).buid();

	}

	@Test
	public void should_insert_join_tweets() throws Exception
	{

		user = em.merge(user);

		user.getRetweets().insert(1, reTweet1);
		user.getRetweets().insert(2, reTweet2);

		List<UUID> savedReTweetsUUIDs = externalJoinWideMapDao.findValuesRange(userId, null, null,
				false, 10);

		assertThat(savedReTweetsUUIDs).hasSize(2);
		assertThat(savedReTweetsUUIDs).containsExactly(reTweet1.getId(), reTweet2.getId());

		Tweet foundReTweet1 = em.find(Tweet.class, reTweet1.getId());
		Tweet foundReTweet2 = em.find(Tweet.class, reTweet2.getId());

		assertThat(foundReTweet1.getId()).isEqualTo(reTweet1.getId());
		assertThat(foundReTweet1.getContent()).isEqualTo(reTweet1.getContent());
		assertThat(foundReTweet2.getId()).isEqualTo(reTweet2.getId());
		assertThat(foundReTweet2.getContent()).isEqualTo(reTweet2.getContent());
	}

	@Test
	public void should_cascade_persist_join_tweet() throws Exception
	{
		user = em.merge(user);

		Tweet transientReTweet = TweetTestBuilder.tweet().randomId().content("unkonwTweet")
				.creator(user).buid();
		user.getRetweets().insert(RandomUtils.nextInt(), transientReTweet);

		List<UUID> savedReTweetsUUIDs = externalJoinWideMapDao.findValuesRange(userId, null, null,
				false, 10);

		assertThat(savedReTweetsUUIDs).hasSize(1);
		assertThat(savedReTweetsUUIDs).containsExactly(transientReTweet.getId());

		Tweet foundRetweet = em.find(Tweet.class, transientReTweet.getId());

		assertThat(foundRetweet).isNotNull();
		assertThat(foundRetweet.getContent()).isEqualTo(transientReTweet.getContent());

	}

	@Test
	public void should_find_join_tweets() throws Exception
	{

		user = em.merge(user);

		user.getRetweets().insert(1, reTweet1);
		user.getRetweets().insert(2, reTweet2);
		user.getRetweets().insert(3, reTweet3);
		user.getRetweets().insert(4, reTweet4);

		List<UUID> savedReTweetsUUIDs = externalJoinWideMapDao.findValuesRange(userId, null, null,
				false, 10);

		assertThat(savedReTweetsUUIDs).hasSize(4);
		assertThat(savedReTweetsUUIDs).containsExactly(reTweet1.getId(), reTweet2.getId(),
				reTweet3.getId(), reTweet4.getId());

		List<KeyValue<Integer, Tweet>> foundReTweetsKeyValues = user.getRetweets().findReverse(2,
				1, 5);

		assertThat(foundReTweetsKeyValues).hasSize(2);

		Tweet foundReTweet1 = foundReTweetsKeyValues.get(0).getValue();
		Tweet foundReTweet2 = foundReTweetsKeyValues.get(1).getValue();

		assertThat(foundReTweetsKeyValues.get(0).getKey()).isEqualTo(2);
		assertThat(foundReTweet1.getId()).isEqualTo(reTweet2.getId());
		assertThat(foundReTweet1.getContent()).isEqualTo(reTweet2.getContent());
		assertThat(foundReTweetsKeyValues.get(1).getKey()).isEqualTo(1);
		assertThat(foundReTweet2.getId()).isEqualTo(reTweet1.getId());
		assertThat(foundReTweet2.getContent()).isEqualTo(reTweet1.getContent());

		List<Tweet> foundReTweetsValues = user.getRetweets().findReverseValues(2, 1, 5);

		assertThat(foundReTweetsValues.get(0).getId()).isEqualTo(reTweet2.getId());
		assertThat(foundReTweetsValues.get(0).getContent()).isEqualTo(reTweet2.getContent());
		assertThat(foundReTweetsValues.get(1).getId()).isEqualTo(reTweet1.getId());
		assertThat(foundReTweetsValues.get(1).getContent()).isEqualTo(reTweet1.getContent());

		List<Integer> foundReTweetsKeys = user.getRetweets().findReverseKeys(2, 1, 5);

		assertThat(foundReTweetsKeys.get(0)).isEqualTo(2);
		assertThat(foundReTweetsKeys.get(1)).isEqualTo(1);
	}

	@Test
	public void should_remove_join_tweet_ids_but_not_tweet_entities() throws Exception
	{

		user = em.merge(user);

		user.getRetweets().insert(1, reTweet1);
		user.getRetweets().insert(2, reTweet2);
		user.getRetweets().insert(3, reTweet3);
		user.getRetweets().insert(4, reTweet4);

		user.getRetweets().remove(2, 4, BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		List<UUID> savedReTweetsUUIDs = externalJoinWideMapDao.findValuesRange(userId, null, null,
				false, 10);

		assertThat(savedReTweetsUUIDs).hasSize(2);
		assertThat(savedReTweetsUUIDs).containsExactly(reTweet1.getId(), reTweet4.getId());

		List<KeyValue<Integer, Tweet>> foundReTweets = user.getRetweets().find(1, 4, 10);

		assertThat(foundReTweets).hasSize(2);

		Tweet foundReTweet1 = foundReTweets.get(0).getValue();
		Tweet foundReTweet2 = foundReTweets.get(1).getValue();

		assertThat(foundReTweet1.getId()).isEqualTo(reTweet1.getId());
		assertThat(foundReTweet1.getContent()).isEqualTo(reTweet1.getContent());
		assertThat(foundReTweet2.getId()).isEqualTo(reTweet4.getId());
		assertThat(foundReTweet2.getContent()).isEqualTo(reTweet4.getContent());

		assertThat(em.find(Tweet.class, reTweet1.getId())).isNotNull();
		assertThat(em.find(Tweet.class, reTweet2.getId())).isNotNull();
		assertThat(em.find(Tweet.class, reTweet3.getId())).isNotNull();
		assertThat(em.find(Tweet.class, reTweet4.getId())).isNotNull();

	}

	@Test
	public void should_iterate_through_tweets() throws Exception
	{
		user = em.merge(user);

		user.getRetweets().insert(1, reTweet1);
		user.getRetweets().insert(2, reTweet2);
		user.getRetweets().insert(3, reTweet3);
		user.getRetweets().insert(4, reTweet4);

		KeyValueIterator<Integer, Tweet> iterator = user.getRetweets().iterator(3, 1, 10,
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.DESCENDING);

		Tweet foundReTweet1 = iterator.next().getValue();
		Tweet foundReTweet2 = iterator.next().getValue();

		assertThat(foundReTweet1.getId()).isEqualTo(reTweet3.getId());
		assertThat(foundReTweet1.getContent()).isEqualTo(reTweet3.getContent());
		assertThat(foundReTweet2.getId()).isEqualTo(reTweet2.getId());
		assertThat(foundReTweet2.getContent()).isEqualTo(reTweet2.getContent());

	}

	@Test
	public void should_remove_all_values_when_entity_is_removed() throws Exception
	{
		user = em.merge(user);

		user.getRetweets().insert(1, reTweet1);
		user.getRetweets().insert(2, reTweet2);
		user.getRetweets().insert(3, reTweet3);
		user.getRetweets().insert(4, reTweet4);

		em.remove(user);

		List<UUID> savedReTweetsUUIDs = externalJoinWideMapDao.findValuesRange(userId, null, null,
				false, 10);

		assertThat(savedReTweetsUUIDs).hasSize(0);

	}

	@Test
	public void should_proxy_join_entity() throws Exception
	{
		user = em.merge(user);
		user.getRetweets().insert(1, reTweet1);

		Tweet tweetProxy = user.getRetweets().get(1);
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getRetweets().findFirst().getValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getRetweets().findFirstValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getRetweets().iterator(null, null, 1).next().getValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);

		tweetProxy = user.getRetweets().iterator(null, null, 1).nextValue();
		assertThat(tweetProxy).isInstanceOf(Factory.class);
	}

	@After
	public void tearDown()
	{
		tweetDao.truncate();
	}
}
