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

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredMessage;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId.Type;
import info.archinnov.achilles.test.integration.entity.ClusteredTweet;
import info.archinnov.achilles.test.integration.entity.ClusteredTweetId;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Date;
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class ClusteredEntityIT2 {
	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "ClusteredTweet",
			"ClusteredMessage");

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	@Test
	public void should_persist_and_find() throws Exception {
		Long userId = RandomUtils.nextLong();
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);

		manager.persist(tweet);

		ClusteredTweet found = manager.find(ClusteredTweet.class, id);

		assertThat(found.getContent()).isEqualTo("this is a tweet");
		assertThat(found.getOriginalAuthorId()).isEqualTo(userId);
		assertThat(found.getIsARetweet()).isFalse();
	}

	@Test
	public void should_merge() throws Exception {
		Long userId = RandomUtils.nextLong();
		Long originalAuthorId = RandomUtils.nextLong();

		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);
		tweet = manager.merge(tweet);

		tweet.setContent("this is a new tweet2");
		tweet.setIsARetweet(true);
		tweet.setOriginalAuthorId(originalAuthorId);

		manager.merge(tweet);

		ClusteredTweet found = manager.find(ClusteredTweet.class, id);

		assertThat(found.getContent()).isEqualTo("this is a new tweet2");
		assertThat(found.getOriginalAuthorId()).isEqualTo(originalAuthorId);
		assertThat(found.getIsARetweet()).isTrue();
	}

	@Test
	public void should_remove() throws Exception {
		Long userId = RandomUtils.nextLong();
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);

		tweet = manager.merge(tweet);

		manager.remove(tweet);

		ClusteredTweet found = manager.find(ClusteredTweet.class, id);

		assertThat(found).isNull();
	}

	@Test
	public void should_refresh() throws Exception {

		Long userId = RandomUtils.nextLong();
		Long originalAuthorId = RandomUtils.nextLong();
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);

		tweet = manager.merge(tweet);

		session.execute("update clusteredtweet set content='New tweet',original_author_id=" + originalAuthorId
				+ ",is_a_retweet=true where user_id=" + userId + " and tweet_id=" + tweetId + " and creation_date="
				+ creationDate.getTime());

		Thread.sleep(100);

		manager.refresh(tweet);

		assertThat(tweet.getContent()).isEqualTo("New tweet");
		assertThat(tweet.getOriginalAuthorId()).isEqualTo(originalAuthorId);
		assertThat(tweet.getIsARetweet()).isTrue();
	}

	@Test
	public void should_persist_and_find_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.TEXT);

		ClusteredMessage message = new ClusteredMessage(messageId, "a message");

		manager.persist(message);

		ClusteredMessage found = manager.find(ClusteredMessage.class, messageId);

		ClusteredMessageId foundCompoundKey = found.getId();
		assertThat(foundCompoundKey.getId()).isEqualTo(id);
		assertThat(foundCompoundKey.getType()).isEqualTo(Type.TEXT);
	}

	@Test
	public void should_merge_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.IMAGE);

		ClusteredMessage message = new ClusteredMessage(messageId, "an image");

		message = manager.merge(message);

		message.setLabel("a JPEG image");

		manager.merge(message);

		ClusteredMessage found = manager.find(ClusteredMessage.class, messageId);

		assertThat(found.getLabel()).isEqualTo("a JPEG image");
	}

	@Test
	public void should_remove_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.AUDIO);

		ClusteredMessage message = new ClusteredMessage(messageId, "an mp3");

		message = manager.merge(message);

		manager.remove(message);

		ClusteredMessage found = manager.find(ClusteredMessage.class, messageId);

		assertThat(found).isNull();
	}

	@Test
	public void should_refresh_entity_having_compound_id_with_enum() throws Exception {
		String label = "a random file";
		String newLabel = "a pdf file";

		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.FILE);

		ClusteredMessage message = new ClusteredMessage(messageId, label);

		message = manager.merge(message);

		String updateQuery = "update ClusteredMessage set label='" + newLabel + "' where id=" + id + " and type='FILE'";

		CQLDaoContext daoContext = Whitebox.getInternalState(manager, CQLDaoContext.class);

		daoContext.execute(new SimpleStatement(updateQuery));

		Thread.sleep(2000);

		manager.refresh(message, ConsistencyLevel.ALL);

		assertThat(message.getLabel()).isEqualTo("a pdf file");
	}
}
