/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Date;
import java.util.UUID;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId.Type;
import info.archinnov.achilles.test.integration.entity.ClusteredTweetEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredTweetId;

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
		Long userId = RandomUtils.nextLong(0,Long.MAX_VALUE);
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);

		manager.insert(tweet);

		ClusteredTweetEntity found = manager.find(ClusteredTweetEntity.class, id);

		assertThat(found.getContent()).isEqualTo("this is a tweet");
		assertThat(found.getOriginalAuthorId()).isEqualTo(userId);
		assertThat(found.getIsARetweet()).isFalse();
	}

	@Test
	public void should_merge() throws Exception {
		Long userId = RandomUtils.nextLong(0,Long.MAX_VALUE);
		Long originalAuthorId = RandomUtils.nextLong(0, Long.MAX_VALUE);

		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);
		manager.insert(tweet);

		final ClusteredTweetEntity proxy = manager.forUpdate(ClusteredTweetEntity.class, tweet.getId());


		proxy.setContent("this is a new tweet2");
		proxy.setIsARetweet(true);
		proxy.setOriginalAuthorId(originalAuthorId);

		manager.update(proxy);

		ClusteredTweetEntity found = manager.find(ClusteredTweetEntity.class, id);

		assertThat(found.getContent()).isEqualTo("this is a new tweet2");
		assertThat(found.getOriginalAuthorId()).isEqualTo(originalAuthorId);
		assertThat(found.getIsARetweet()).isTrue();
	}

	@Test
	public void should_delete() throws Exception {
		Long userId = RandomUtils.nextLong(0,Long.MAX_VALUE);
		UUID tweetId = UUIDGen.getTimeUUID();
		Date creationDate = new Date();

		ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

		ClusteredTweetEntity tweet = new ClusteredTweetEntity(id, "this is a tweet", userId, false);

		manager.insert(tweet);

		manager.delete(tweet);

		ClusteredTweetEntity found = manager.find(ClusteredTweetEntity.class, id);

		assertThat(found).isNull();
	}

	@Test
	public void should_persist_and_find_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.TEXT);

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, "a message");

		manager.insert(message);

		ClusteredMessageEntity found = manager.find(ClusteredMessageEntity.class, messageId);

		ClusteredMessageId foundCompoundKey = found.getId();
		assertThat(foundCompoundKey.getId()).isEqualTo(id);
		assertThat(foundCompoundKey.getType()).isEqualTo(Type.TEXT);
	}

	@Test
	public void should_update_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.IMAGE);

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, "an image");

		manager.insert(message);

		final ClusteredMessageEntity proxy = manager.forUpdate(ClusteredMessageEntity.class, message.getId());

		proxy.setLabel("a JPEG image");

		manager.update(proxy);

		ClusteredMessageEntity found = manager.find(ClusteredMessageEntity.class, messageId);

		assertThat(found.getLabel()).isEqualTo("a JPEG image");
	}

	@Test
	public void should_delete_entity_having_compound_id_with_enum() throws Exception {
		long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		ClusteredMessageId messageId = new ClusteredMessageId(id, Type.AUDIO);

		ClusteredMessageEntity message = new ClusteredMessageEntity(messageId, "an mp3");

		manager.insert(message);

		manager.delete(message);

		ClusteredMessageEntity found = manager.find(ClusteredMessageEntity.class, messageId);

		assertThat(found).isNull();
	}
}
