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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.builders.CompositeTestBuilder;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithJoinCollectionAndMap;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JoinCollectionAndMapIT {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(
			Steps.AFTER_TEST, "Tweet", "User", "EntityWithJoinCollectionAndMap");

	private ThriftEntityManager em = resource.getEm();

	private ThriftGenericEntityDao beanDao = resource
			.getEntityDao(
					normalizerAndValidateColumnFamilyName(EntityWithJoinCollectionAndMap.class
							.getCanonicalName()), Long.class);

	private Tweet tweet1, tweet2, tweet3, tweet4, tweet5;

	private User friend1, friend2;

	private Long beanId = RandomUtils.nextLong();

	private EntityWithJoinCollectionAndMap bean;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp() {
		bean = new EntityWithJoinCollectionAndMap();
		bean.setId(beanId);

		tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
		tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();
		tweet3 = TweetTestBuilder.tweet().randomId().content("tweet3").buid();
		tweet4 = TweetTestBuilder.tweet().randomId().content("tweet4").buid();
		tweet5 = TweetTestBuilder.tweet().randomId().content("tweet5").buid();

		friend1 = UserTestBuilder.user().randomId().firstname("friend1").buid();
		friend2 = UserTestBuilder.user().randomId().firstname("friend2").buid();
	}

	@Test
	public void should_persist_join_collection_and_map() throws Exception {
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
				.values(JOIN_SET.flag(), "friends").equality(EQUAL)
				.buildForQuery();
		Composite endFriendsComp = CompositeTestBuilder.builder()
				.values(JOIN_SET.flag(), "friends")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> friendsColumns = beanDao
				.findColumnsRange(beanId, startFriendsComp, endFriendsComp,
						false, 20);

		assertThat(friendsColumns).hasSize(2);
		assertThat(readLong(friendsColumns.get(0).left.get(2, STRING_SRZ)))
				.isIn(friend1.getId(), friend2.getId());
		assertThat(readLong(friendsColumns.get(1).left.get(2, STRING_SRZ)))
				.isIn(friend1.getId(), friend2.getId());

		Composite startTweetsComp = CompositeTestBuilder.builder()
				.values(JOIN_LIST.flag(), "tweets").equality(EQUAL)
				.buildForQuery();
		Composite endTweetsComp = CompositeTestBuilder.builder()
				.values(JOIN_LIST.flag(), "tweets")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> tweetsColumns = beanDao.findColumnsRange(
				beanId, startTweetsComp, endTweetsComp, false, 20);

		assertThat(tweetsColumns).hasSize(2);
		assertThat(readUUID(tweetsColumns.get(0).right)).isEqualTo(
				tweet1.getId());
		assertThat(readUUID(tweetsColumns.get(1).right)).isEqualTo(
				tweet2.getId());

		Composite startTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline").equality(EQUAL)
				.buildForQuery();
		Composite endTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> timelineColumns = beanDao
				.findColumnsRange(beanId, startTimelineComp, endTimelineComp,
						false, 20);

		assertThat(timelineColumns).hasSize(3);
		assertThat(timelineColumns.get(0).left.get(2, STRING_SRZ)).isEqualTo(
				"3");
		assertThat(timelineColumns.get(1).left.get(2, STRING_SRZ)).isEqualTo(
				"4");
		assertThat(timelineColumns.get(2).left.get(2, STRING_SRZ)).isEqualTo(
				"5");

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
	public void should_exception_when_join_user_does_not_exist()
			throws Exception {
		Set<User> friends = new HashSet<User>();
		friends.add(friend1);

		bean.setFriends(friends);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The entity '"
						+ User.class.getCanonicalName()
						+ "' with id '"
						+ friend1.getId()
						+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
		em.persist(bean);
	}

	@Test
	public void should_merge_join_collection_and_map() throws Exception {

		Map<Integer, Tweet> timeline = new HashMap<Integer, Tweet>();
		timeline.put(3, tweet3);

		bean.setTweets(Arrays.asList(tweet1, tweet2));
		bean.setTimeline(timeline);

		bean = em.merge(bean);

		Composite startTweetsComp = CompositeTestBuilder.builder()
				.values(JOIN_LIST.flag(), "tweets").equality(EQUAL)
				.buildForQuery();
		Composite endTweetsComp = CompositeTestBuilder.builder()
				.values(JOIN_LIST.flag(), "tweets")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> tweetsColumns = beanDao.findColumnsRange(
				beanId, startTweetsComp, endTweetsComp, false, 20);

		assertThat(tweetsColumns).hasSize(2);
		assertThat(readUUID(tweetsColumns.get(0).right)).isEqualTo(
				tweet1.getId());
		assertThat(readUUID(tweetsColumns.get(1).right)).isEqualTo(
				tweet2.getId());

		Composite startTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline").equality(EQUAL)
				.buildForQuery();
		Composite endTimelineComp = CompositeTestBuilder.builder()
				.values(JOIN_MAP.flag(), "timeline")
				.equality(GREATER_THAN_EQUAL).buildForQuery();

		List<Pair<Composite, String>> timelineColumns = beanDao
				.findColumnsRange(beanId, startTimelineComp, endTimelineComp,
						false, 20);

		assertThat(timelineColumns).hasSize(1);
		assertThat(readUUID(timelineColumns.get(0).right)).isEqualTo(
				tweet3.getId());

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent())
				.isEqualTo(tweet1.getContent());
		assertThat(em.find(Tweet.class, tweet2.getId()).getContent())
				.isEqualTo(tweet2.getContent());
		assertThat(em.find(Tweet.class, tweet3.getId()).getContent())
				.isEqualTo(tweet3.getContent());
	}

	@Test
	public void should_update_join_entity_after_merge() throws Exception {
		em.persist(tweet1);

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent())
				.isEqualTo("tweet1");

		tweet1.setContent("updated_content");

		bean.setTweets(Arrays.asList(tweet1));

		em.merge(bean);

		assertThat(em.find(Tweet.class, tweet1.getId()).getContent())
				.isEqualTo("updated_content");
	}

	@Test
	public void should_find_bean_after_persist() throws Exception {
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

		bean = em.find(EntityWithJoinCollectionAndMap.class, beanId);

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
		assertThat(foundTweets.get(0).getContent()).isEqualTo(
				tweet1.getContent());
		assertThat(foundTweets.get(1)).isInstanceOf(Factory.class);
		assertThat(foundTweets.get(1).getId()).isEqualTo(tweet2.getId());
		assertThat(foundTweets.get(1).getContent()).isEqualTo(
				tweet2.getContent());

		Map<Integer, Tweet> foundTimeline = bean.getTimeline();

		assertThat(foundTimeline).hasSize(3);
		assertThat(foundTimeline.get(3)).isInstanceOf(Factory.class);
		assertThat(foundTimeline.get(3).getId()).isEqualTo(tweet3.getId());
		assertThat(foundTimeline.get(3).getContent()).isEqualTo(
				tweet3.getContent());
		assertThat(foundTimeline.get(4)).isInstanceOf(Factory.class);
		assertThat(foundTimeline.get(4).getId()).isEqualTo(tweet4.getId());
		assertThat(foundTimeline.get(4).getContent()).isEqualTo(
				tweet4.getContent());
		assertThat(foundTimeline.get(5)).isInstanceOf(Factory.class);
		assertThat(foundTimeline.get(5).getId()).isEqualTo(tweet5.getId());
		assertThat(foundTimeline.get(5).getContent()).isEqualTo(
				tweet5.getContent());
	}

	@Test
	public void should_refresh_join_entity() throws Exception {
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
	public void should_not_cascade_remove_join_entity() throws Exception {
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
	public void should_unproxy_join_entity() throws Exception {
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

		bean = em.unwrap(bean);

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

	private Long readLong(String value) throws Exception {
		return this.objectMapper.readValue(value, Long.class);
	}

	private UUID readUUID(String value) throws Exception {
		return this.objectMapper.readValue(value, UUID.class);
	}
}
