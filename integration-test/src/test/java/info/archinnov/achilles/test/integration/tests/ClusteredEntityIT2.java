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

import java.util.Date;
import java.util.UUID;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId.Type;
import info.archinnov.achilles.test.integration.entity.ClusteredTweetEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredTweetId;
import info.archinnov.achilles.type.ConsistencyLevel;

public class ClusteredEntityIT2 {

	private static final String CLUSTERED_TWEET_TABLE = ClusteredTweetEntity.class.getSimpleName();
	private static final String CLUSTERED_MESSAGE_TABLE = ClusteredMessageEntity.class.getSimpleName();
	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
			CLUSTERED_TWEET_TABLE, CLUSTERED_MESSAGE_TABLE);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	@Test
	public void should_persist_and_find() throws Exception {
		Long userId = RandomUtils.nextLong();
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);

		manager.persist(tweet);

		ClusteredTweetEntity found = manager.find(ClusteredTweetEntity.class, id);

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

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);
		tweet = manager.persist(tweet);

		tweet.setContent("this is a new tweet2");
		tweet.setIsARetweet(true);
		tweet.setOriginalAuthorId(originalAuthorId);

		manager.update(tweet);

		ClusteredTweetEntity found = manager.find(ClusteredTweetEntity.class, id);

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

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);

		tweet = manager.persist(tweet);

		manager.remove(tweet);

		ClusteredTweetEntity found = manager.find(ClusteredTweetEntity.class, id);

		assertThat(found).isNull();
	}

	@Test
	public void should_refresh() throws Exception {

		Long userId = RandomUtils.nextLong();
		Long originalAuthorId = RandomUtils.nextLong();
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);

		tweet = manager.persist(tweet);

		session.execute("update " + CLUSTERED_TWEET_TABLE + " set content='New tweet',original_author_id="
				+ originalAuthorId + ",is_a_retweet=true where user_id=" + userId + " and tweet_id=" + tweetId
				+ " and creation_date=" + creationDate.getTime());

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

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, "a message");

		manager.persist(message);

		ClusteredMessageEntity found = manager.find(ClusteredMessageEntity.class, messageId);

		ClusteredMessageId foundCompoundKey = found.getId();
		assertThat(foundCompoundKey.getId()).isEqualTo(id);
		assertThat(foundCompoundKey.getType()).isEqualTo(Type.TEXT);
	}

	@Test
	public void should_update_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.IMAGE);

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, "an image");

		message = manager.persist(message);

		message.setLabel("a JPEG image");

		manager.update(message);

		ClusteredMessageEntity found = manager.find(ClusteredMessageEntity.class, messageId);

		assertThat(found.getLabel()).isEqualTo("a JPEG image");
	}

	@Test
	public void should_remove_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.AUDIO);

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, "an mp3");

		message = manager.persist(message);

		manager.remove(message);

		ClusteredMessageEntity found = manager.find(ClusteredMessageEntity.class, messageId);

		assertThat(found).isNull();
	}

	@Test
	public void should_refresh_entity_having_compound_id_with_enum() throws Exception {
		String label = "a random file";
		String newLabel = "a pdf file";

		long id = RandomUtils.nextLong();
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.FILE);

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, label);

		message = manager.persist(message);

		String updateQuery = "update " + CLUSTERED_MESSAGE_TABLE + " set label='" + newLabel + "' where id=" + id
				+ " and type='FILE'";

		session.execute(new SimpleStatement(updateQuery));

		Thread.sleep(200);

		manager.refresh(message, ConsistencyLevel.ALL);

		assertThat(message.getLabel()).isEqualTo("a pdf file");
	}
}
