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

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;

import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JoinColumnIT {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(
			Steps.AFTER_TEST, "Tweet", "User");

	private ThriftEntityManager em = resource.getEm();

	private ThriftGenericEntityDao tweetDao = resource.getEntityDao(
			normalizerAndValidateColumnFamilyName(Tweet.class
					.getCanonicalName()), UUID.class);

	private Tweet tweet;
	private User creator;
	private Long creatorId = RandomUtils.nextLong();

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp() {
		creator = UserTestBuilder.user().id(creatorId).firstname("fn")
				.lastname("ln").buid();
	}

	@Test
	public void should_persist_user_and_then_tweet() throws Exception {

		em.persist(creator);

		tweet = TweetTestBuilder.tweet().randomId().content("this is a tweet")
				.creator(creator).buid();

		em.persist(tweet);

		Composite startComp = new Composite();
		startComp.addComponent(0, JOIN_SIMPLE.flag(), ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, JOIN_SIMPLE.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = tweetDao.findColumnsRange(
				tweet.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);

		Pair<Composite, String> creator = columns.get(0);
		assertThat(readLong(creator.right)).isEqualTo(creatorId);

	}

	@Test
	public void should_find_user_from_tweet_after_persist() throws Exception {

		em.persist(creator);

		tweet = TweetTestBuilder.tweet().randomId().content("this is a tweet")
				.creator(creator).buid();

		em.persist(tweet);

		tweet = em.find(Tweet.class, tweet.getId());

		User joinUser = tweet.getCreator();

		assertThat(joinUser).isNotNull();
		assertThat(joinUser).isInstanceOf(Factory.class);
		assertThat(joinUser.getId()).isEqualTo(creatorId);
		assertThat(joinUser.getFirstname()).isEqualTo("fn");
		assertThat(joinUser.getLastname()).isEqualTo("ln");

	}

	@Test
	public void should_find_user_unchanged_from_tweet_after_merge()
			throws Exception {

		em.persist(creator);

		tweet = TweetTestBuilder.tweet().randomId().content("this is a tweet")
				.creator(creator).buid();

		creator.setFirstname("dfvdfv");
		creator.setLastname("fgbkl");

		tweet = em.merge(tweet);

		User joinUser = tweet.getCreator();

		assertThat(joinUser).isNotNull();
		assertThat(joinUser).isInstanceOf(Factory.class);
		assertThat(joinUser.getId()).isEqualTo(creatorId);
		assertThat(joinUser.getFirstname()).isEqualTo("fn");
		assertThat(joinUser.getLastname()).isEqualTo("ln");

	}

	@Test
	public void should_find_user_modified_from_tweet_after_refresh()
			throws Exception {

		em.persist(creator);

		tweet = TweetTestBuilder.tweet().randomId().content("this is a tweet")
				.creator(creator).buid();

		tweet = em.merge(tweet);

		User joinUser = tweet.getCreator();

		joinUser.setFirstname("changed_fn");
		joinUser.setLastname("changed_ln");

		em.merge(joinUser);

		em.refresh(tweet);

		joinUser = tweet.getCreator();

		assertThat(joinUser).isNotNull();
		assertThat(joinUser).isInstanceOf(Factory.class);
		assertThat(joinUser.getId()).isEqualTo(creatorId);
		assertThat(joinUser.getFirstname()).isEqualTo("changed_fn");
		assertThat(joinUser.getLastname()).isEqualTo("changed_ln");

	}

	@Test
	public void should_exception_when_persisting_join_user_without_existing_entity_in_db()
			throws Exception {

		creator = UserTestBuilder.user().id(RandomUtils.nextLong()).buid();
		tweet = TweetTestBuilder.tweet().randomId().content("this is a tweet")
				.creator(creator).buid();

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The entity '"
						+ User.class.getCanonicalName()
						+ "' with id '"
						+ creator.getId()
						+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST");
		em.persist(tweet);
	}

	@Test
	public void should_unproxy_entity() throws Exception {
		em.persist(creator);

		tweet = TweetTestBuilder.tweet().randomId().content("this is a tweet")
				.creator(creator).buid();

		tweet = em.merge(tweet);
		em.initialize(tweet);
		tweet = em.unwrap(tweet);

		assertThat(tweet).isNotInstanceOf(Factory.class);
		assertThat(tweet.getCreator()).isNotInstanceOf(Factory.class);
	}

	@Test
	public void should_persist_user_and_referrer() throws Exception {

		creator = UserTestBuilder.user().randomId().firstname("creator").buid();
		User referrer = UserTestBuilder.user().randomId().firstname("referrer")
				.buid();

		creator.setReferrer(referrer);
		referrer.setReferrer(creator);

		em.persist(creator);

		User foundCreator = em.find(User.class, creator.getId());

		User foundReferrer = foundCreator.getReferrer();
		assertThat(foundReferrer.getId()).isEqualTo(referrer.getId());
		assertThat(foundReferrer.getFirstname()).isEqualTo("referrer");
	}

	@Test
	public void should_merge_user_and_referrer() throws Exception {

		creator = UserTestBuilder.user().randomId().firstname("creator").buid();
		User referrer = UserTestBuilder.user().randomId().firstname("referrer")
				.buid();

		creator.setReferrer(referrer);
		referrer.setReferrer(creator);

		em.persist(creator);

		User foundCreator = em.find(User.class, creator.getId());
		User foundReferrer = foundCreator.getReferrer();

		foundCreator.setFirstname("modified_creator");
		foundReferrer.setFirstname("modified_referrer");

		em.merge(foundCreator);

		User modifiedCreator = em.find(User.class, creator.getId());
		User modifiedReferrer = foundCreator.getReferrer();

		assertThat(modifiedCreator.getFirstname())
				.isEqualTo("modified_creator");
		assertThat(modifiedReferrer.getFirstname()).isEqualTo(
				"modified_referrer");
	}

	@Test
	public void should_persist_transient_user_and_referrer_with_merge()
			throws Exception {

		creator = UserTestBuilder.user().randomId().firstname("creator").buid();
		User referrer = UserTestBuilder.user().randomId().firstname("referrer")
				.buid();

		creator.setReferrer(referrer);
		referrer.setReferrer(creator);

		em.merge(creator);

		User foundCreator = em.find(User.class, creator.getId());

		User foundReferrer = foundCreator.getReferrer();
		assertThat(foundReferrer.getId()).isEqualTo(referrer.getId());
		assertThat(foundReferrer.getFirstname()).isEqualTo("referrer");
	}

	private Long readLong(String value) throws Exception {
		return this.objectMapper.readValue(value, Long.class);
	}

}
