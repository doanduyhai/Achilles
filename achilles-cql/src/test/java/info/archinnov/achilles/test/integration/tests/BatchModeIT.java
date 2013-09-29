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

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.CQLBatchingFlushContext;
import info.archinnov.achilles.entity.manager.CQLBatchingEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;

public class BatchModeIT {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
			"Tweet", "User");

	private CQLEntityManagerFactory emf = resource.getFactory();

	private CQLEntityManager em = resource.getEm();

	private Session session = resource.getNativeSession();

	private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	private User user;

	private Long userId = RandomUtils.nextLong();

	@Before
	public void setUp() {
		user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();
	}

	@Test
	public void should_batch_counters() throws Exception {
		// Start batch
		CQLBatchingEntityManager batchEm = emf.createBatchingEntityManager();
		batchEm.startBatch();

		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

		entity = batchEm.merge(entity);

		entity.setLabel("label");

		Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("welcomeTweet").buid();
		entity.setWelcomeTweet(welcomeTweet);

		entity.getVersion().incr(10L);
		batchEm.merge(entity);

		Row result = session.execute("SELECT label from CompleteBean where id=" + entity.getId()).one();
		assertThat(result).isNull();

		result = session.execute(
				"SELECT counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + entity.getId() + "' and property_name='version'").one();
		assertThat(result.getLong("counter_value")).isEqualTo(10L);

		// Flush
		batchEm.endBatch();

		result = session.execute("SELECT label from CompleteBean where id=" + entity.getId()).one();
		assertThat(result.getString("label")).isEqualTo("label");

		result = session.execute(
				"SELECT counter_value from achilles_counter_table where fqcn='" + CompleteBean.class.getCanonicalName()
						+ "' and primary_key='" + entity.getId() + "' and property_name='version'").one();
		assertThat(result.getLong("counter_value")).isEqualTo(10L);
		assertThatBatchContextHasBeenReset(batchEm);
	}

	@Test
	public void should_batch_several_entities() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();

		// Start batch
		CQLBatchingEntityManager batchEm = emf.createBatchingEntityManager();
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
	}

	@Test
	public void should_reinit_batch_context_after_exception() throws Exception {
		User user = UserTestBuilder.user().id(123456494L).firstname("firstname").lastname("lastname").buid();
		Tweet tweet = TweetTestBuilder.tweet().randomId().content("simple_tweet").creator(user).buid();

		// Start batch
		CQLBatchingEntityManager batchEm = emf.createBatchingEntityManager();
		batchEm.startBatch();

		try {
			batchEm.persist(tweet);
		} catch (AchillesException e) {
			batchEm.cleanBatch();
			assertThatBatchContextHasBeenReset(batchEm);

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
	public void should_batch_with_custom_consistency_level() throws Exception {
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();
		Tweet tweet3 = TweetTestBuilder.tweet().randomId().content("simple_tweet3").buid();

		em.persist(tweet1);

		// Start batch
		CQLBatchingEntityManager batchEm = emf.createBatchingEntityManager();
		batchEm.startBatch();

		batchEm.startBatch(QUORUM);

		logAsserter.prepareLogLevel();

		Tweet foundTweet1 = batchEm.find(Tweet.class, tweet1.getId());

		assertThat(foundTweet1.getContent()).isEqualTo(tweet1.getContent());

		batchEm.persist(tweet2);
		batchEm.persist(tweet3);

		batchEm.endBatch();

		logAsserter.assertConsistencyLevels(QUORUM, QUORUM);
		assertThatBatchContextHasBeenReset(batchEm);
	}

	@Test
	public void should_reinit_batch_context_and_consistency_after_exception() throws Exception {
		Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
		Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();

		em.persist(tweet1);

		// Start batch
		CQLBatchingEntityManager batchEm = emf.createBatchingEntityManager();
		batchEm.startBatch();

		batchEm.startBatch(EACH_QUORUM);
		batchEm.persist(tweet2);

		try {
			batchEm.endBatch();
		} catch (Exception e) {
			assertThatBatchContextHasBeenReset(batchEm);
		}

		Thread.sleep(1000);
		logAsserter.prepareLogLevel();
		batchEm.persist(tweet2);
		batchEm.endBatch();
		logAsserter.assertConsistencyLevels(ONE, ONE);
	}

	private void assertThatBatchContextHasBeenReset(CQLBatchingEntityManager batchEm) {
		CQLBatchingFlushContext flushContext = Whitebox.getInternalState(batchEm, CQLBatchingFlushContext.class);
		Optional<ConsistencyLevel> consistencyLevel = Whitebox.getInternalState(flushContext, "consistencyLevel");
		List<BoundStatementWrapper> boundStatementWrappers = Whitebox.getInternalState(flushContext,
				"boundStatementWrappers");

		assertThat(consistencyLevel).isNull();
		assertThat(boundStatementWrappers).isEmpty();
	}
}
