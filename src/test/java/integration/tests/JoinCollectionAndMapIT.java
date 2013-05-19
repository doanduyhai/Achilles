package integration.tests;

import static info.archinnov.achilles.columnFamily.AchillesTableHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getEntityDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.Pair;
import info.archinnov.achilles.exception.AchillesException;
import integration.tests.entity.BeanWithJoinCollectionAndMap;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.CompositeTestBuilder;

/**
 * JoinColumnIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinCollectionAndMapIT
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private ThriftGenericEntityDao tweetDao = getEntityDao(
			normalizerAndValidateColumnFamilyName(Tweet.class.getCanonicalName()), UUID.class);

	private ThriftGenericEntityDao userDao = getEntityDao(
			normalizerAndValidateColumnFamilyName(User.class.getCanonicalName()), Long.class);

	private ThriftGenericEntityDao beanDao = getEntityDao(
			normalizerAndValidateColumnFamilyName(BeanWithJoinCollectionAndMap.class
					.getCanonicalName()),
			Long.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private Tweet tweet1, tweet2, tweet3, tweet4, tweet5;

	private User friend1, friend2;

	private Long beanId = RandomUtils.nextLong();

	private BeanWithJoinCollectionAndMap bean;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		bean = new BeanWithJoinCollectionAndMap();
		bean.setId(beanId);

		tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
		tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();
		tweet3 = TweetTestBuilder.tweet().randomId().content("tweet3").buid();
		tweet4 = TweetTestBuilder.tweet().randomId().content("tweet4").buid();
		tweet5 = TweetTestBuilder.tweet().randomId().content("tweet5").buid();

		friend1 = UserTestBuilder.user().id(1L).firstname("friend1").buid();
		friend2 = UserTestBuilder.user().id(2L).firstname("friend2").buid();
	}

	@Test
	public void should_persist_join_collection_and_map() throws Exception
	{
		em.persist(friend1);
		em.persist(friend2);

		Set<User> friends = new HashSet<User>();
		friends.add(friend1);
		friends.add(friend2);

		bean.setTweets(Arrays.asList(tweet1, tweet2));
		bean.setFriends(friends);

		Map<Integer, Tweet> timeline = new HashMap<Integer, Tweet>();

		timeline.put(3, tweet3);
		timeline.put(4, tweet4);
		timeline.put(5, tweet5);

		bean.setTimeline(timeline);

		em.persist(bean);

		Composite startFriendsComp = CompositeTestBuilder.builder()
				.values(JOIN_SET.flag(), "friends").equality(EQUAL).buildForQuery();
		Composite endFriendsComp = CompositeTestBuilder.builder()
				.values(JOIN_SET.flag(), "friends").equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> friendsColumns = beanDao.findColumnsRange(beanId,
				startFriendsComp, endFriendsComp, false, 20);

		assertThat(friendsColumns).hasSize(2);
		assertThat(readLong(friendsColumns.get(0).right)).isIn(friend1.getId(), friend2.getId());
		assertThat(readLong(friendsColumns.get(1).right)).isIn(friend1.getId(), friend2.getId());

		Composite startTweetsComp = CompositeTestBuilder.builder()
				.values(JOIN_LIST.flag(), "tweets").equality(EQUAL).buildForQuery();
		Composite endTweetsComp = CompositeTestBuilder.builder().values(JOIN_LIST.flag(), "tweets")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> tweetsColumns = beanDao.findColumnsRange(beanId,
				startTweetsComp, endTweetsComp, false, 20);

		assertThat(tweetsColumns).hasSize(2);
		assertThat(readUUID(tweetsColumns.get(0).right)).isEqualTo(tweet1.getId());
		assertThat(readUUID(tweetsColumns.get(1).right)).isEqualTo(tweet2.getId());

		Composite startTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline").equality(EQUAL).buildForQuery();
		Composite endTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline").equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> timelineColumns = beanDao.findColumnsRange(beanId,
				startTimelineComp, endTimelineComp, false, 20);

		assertThat(timelineColumns).hasSize(3);
		assertThat(readKeyValue(timelineColumns.get(0).right).getKey()).isEqualTo(3);
		assertThat(readKeyValue(timelineColumns.get(1).right).getKey()).isEqualTo(4);
		assertThat(readKeyValue(timelineColumns.get(2).right).getKey()).isEqualTo(5);

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent())
				.isEqualTo(tweet1.getContent());
		assertThat(em.find(Tweet.class, tweet2.getId()).getContent())
				.isEqualTo(tweet2.getContent());
		assertThat(em.find(Tweet.class, tweet3.getId()).getContent())
				.isEqualTo(tweet3.getContent());
		assertThat(em.find(Tweet.class, tweet4.getId()).getContent())
				.isEqualTo(tweet4.getContent());
		assertThat(em.find(Tweet.class, tweet5.getId()).getContent())
				.isEqualTo(tweet5.getContent());

	}

	@Test
	public void should_exception_when_join_user_does_not_exist() throws Exception
	{
		Set<User> friends = new HashSet<User>();
		friends.add(friend1);

		bean.setFriends(friends);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The entity '"
						+ User.class.getCanonicalName()
						+ "' with id '1' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
		em.persist(bean);
	}

	@Test
	public void should_merge_join_collection_and_map() throws Exception
	{

		Map<Integer, Tweet> timeline = new HashMap<Integer, Tweet>();
		timeline.put(3, tweet3);

		bean.setTweets(Arrays.asList(tweet1, tweet2));
		bean.setTimeline(timeline);

		bean = em.merge(bean);

		Composite startTweetsComp = CompositeTestBuilder.builder()
				.values(JOIN_LIST.flag(), "tweets").equality(EQUAL).buildForQuery();
		Composite endTweetsComp = CompositeTestBuilder.builder().values(JOIN_LIST.flag(), "tweets")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> tweetsColumns = beanDao.findColumnsRange(beanId,
				startTweetsComp, endTweetsComp, false, 20);

		assertThat(tweetsColumns).hasSize(2);
		assertThat(readUUID(tweetsColumns.get(0).right)).isEqualTo(tweet1.getId());
		assertThat(readUUID(tweetsColumns.get(1).right)).isEqualTo(tweet2.getId());

		Composite startTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline").equality(EQUAL).buildForQuery();
		Composite endTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline").equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> timelineColumns = beanDao.findColumnsRange(beanId,
				startTimelineComp, endTimelineComp, false, 20);

		assertThat(timelineColumns).hasSize(1);
		assertThat(readUUID(readKeyValue(timelineColumns.get(0).right).getValue())).isEqualTo(
				tweet3.getId());

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent())
				.isEqualTo(tweet1.getContent());
		assertThat(em.find(Tweet.class, tweet2.getId()).getContent())
				.isEqualTo(tweet2.getContent());
		assertThat(em.find(Tweet.class, tweet3.getId()).getContent())
				.isEqualTo(tweet3.getContent());
	}

	@Test
	public void should_update_join_entity_after_merge() throws Exception
	{
		em.persist(tweet1);

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent()).isEqualTo("tweet1");

		tweet1.setContent("updated_content");

		bean.setTweets(Arrays.asList(tweet1));

		em.merge(bean);

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent()).isEqualTo("updated_content");
	}

	@Test
	public void should_find_bean_after_persist() throws Exception
	{
		em.persist(friend1);
		em.persist(friend2);

		Set<User> friends = new HashSet<User>();
		friends.add(friend1);
		friends.add(friend2);

		bean.setTweets(Arrays.asList(tweet1, tweet2));
		bean.setFriends(friends);

		Map<Integer, Tweet> timeline = new HashMap<Integer, Tweet>();

		timeline.put(3, tweet3);
		timeline.put(4, tweet4);
		timeline.put(5, tweet5);

		bean.setTimeline(timeline);

		em.persist(bean);

		bean = em.find(BeanWithJoinCollectionAndMap.class, beanId);

		Set<User> foundFriends = bean.getFriends();

		assertThat(foundFriends).hasSize(2);

		User foundFriend1 = foundFriends.iterator().next();
		User foundFriend2 = foundFriends.iterator().next();

		assertThat(foundFriend1).isInstanceOf(Factory.class);
		assertThat(foundFriend2).isInstanceOf(Factory.class);
		assertThat(foundFriend1.getFirstname()).isIn("friend1", "friend2");
		assertThat(foundFriend2.getFirstname()).isIn("friend1", "friend2");

		List<Tweet> foundTweets = bean.getTweets();

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0)).isInstanceOf(Factory.class);
		assertThat(foundTweets.get(0).getId()).isEqualTo(tweet1.getId());
		assertThat(foundTweets.get(0).getContent()).isEqualTo(tweet1.getContent());
		assertThat(foundTweets.get(1)).isInstanceOf(Factory.class);
		assertThat(foundTweets.get(1).getId()).isEqualTo(tweet2.getId());
		assertThat(foundTweets.get(1).getContent()).isEqualTo(tweet2.getContent());

		Map<Integer, Tweet> foundTimeline = bean.getTimeline();

		assertThat(foundTimeline).hasSize(3);
		assertThat(foundTimeline.get(3)).isInstanceOf(Factory.class);
		assertThat(foundTimeline.get(3).getId()).isEqualTo(tweet3.getId());
		assertThat(foundTimeline.get(3).getContent()).isEqualTo(tweet3.getContent());
		assertThat(foundTimeline.get(4)).isInstanceOf(Factory.class);
		assertThat(foundTimeline.get(4).getId()).isEqualTo(tweet4.getId());
		assertThat(foundTimeline.get(4).getContent()).isEqualTo(tweet4.getContent());
		assertThat(foundTimeline.get(5)).isInstanceOf(Factory.class);
		assertThat(foundTimeline.get(5).getId()).isEqualTo(tweet5.getId());
		assertThat(foundTimeline.get(5).getContent()).isEqualTo(tweet5.getContent());
	}

	@Test
	public void should_refresh_join_entity() throws Exception
	{
		em.persist(friend1);

		Set<User> friends = new HashSet<User>();
		friends.add(friend1);
		bean.setFriends(friends);

		bean = em.merge(bean);

		friend1.setFirstname("updated_firstname");
		em.merge(friend1);

		em.refresh(bean);

		User friend = bean.getFriends().iterator().next();

		assertThat(friend).isInstanceOf(Factory.class);
		assertThat(friend.getFirstname()).isEqualTo("updated_firstname");
	}

	@Test
	public void should_not_cascade_remove_join_entity() throws Exception
	{
		Map<Integer, Tweet> timeline = new HashMap<Integer, Tweet>();

		timeline.put(3, tweet3);
		timeline.put(4, tweet4);

		bean.setTimeline(timeline);

		bean = em.merge(bean);

		em.remove(bean);

		assertThat(em.find(Tweet.class, tweet3.getId()).getContent())
				.isEqualTo(tweet3.getContent());
		assertThat(em.find(Tweet.class, tweet4.getId()).getContent())
				.isEqualTo(tweet4.getContent());

	}

	@Test
	public void should_unproxy_join_entity() throws Exception
	{
		em.persist(friend1);
		em.persist(friend2);

		Set<User> friends = new HashSet<User>();
		friends.add(friend1);
		friends.add(friend2);

		bean.setTweets(Arrays.asList(tweet1, tweet2));
		bean.setFriends(friends);

		Map<Integer, Tweet> timeline = new HashMap<Integer, Tweet>();

		timeline.put(3, tweet3);
		timeline.put(4, tweet4);
		timeline.put(5, tweet5);

		bean.setTimeline(timeline);

		em.merge(bean);

		bean = em.unproxy(bean);

		Set<User> foundFriends = bean.getFriends();

		assertThat(foundFriends).hasSize(2);

		User foundFriend1 = foundFriends.iterator().next();
		User foundFriend2 = foundFriends.iterator().next();

		assertThat(foundFriend1).isNotInstanceOf(Factory.class);
		assertThat(foundFriend2).isNotInstanceOf(Factory.class);

		List<Tweet> foundTweets = bean.getTweets();

		assertThat(foundTweets.get(0)).isNotInstanceOf(Factory.class);
		assertThat(foundTweets.get(1)).isNotInstanceOf(Factory.class);

		Map<Integer, Tweet> foundTimeline = bean.getTimeline();

		assertThat(foundTimeline.get(3)).isNotInstanceOf(Factory.class);
		assertThat(foundTimeline.get(4)).isNotInstanceOf(Factory.class);
		assertThat(foundTimeline.get(5)).isNotInstanceOf(Factory.class);

	}

	private Long readLong(String value) throws Exception
	{
		return this.objectMapper.readValue(value, Long.class);
	}

	private UUID readUUID(String value) throws Exception
	{
		return this.objectMapper.readValue(value, UUID.class);
	}

	@SuppressWarnings("unchecked")
	private KeyValue<Integer, String> readKeyValue(String value) throws Exception
	{
		return (KeyValue<Integer, String>) this.objectMapper.readValue(value, KeyValue.class);
	}

	@After
	public void tearDown()
	{
		tweetDao.truncate();
		userDao.truncate();
		beanDao.truncate();
	}
}
