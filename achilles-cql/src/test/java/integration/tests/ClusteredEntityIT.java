package integration.tests;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.common.CQLCassandraDaoTest.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import integration.tests.entity.ClusteredMessage;
import integration.tests.entity.ClusteredMessageId;
import integration.tests.entity.ClusteredMessageId.Type;
import integration.tests.entity.ClusteredTweet;
import integration.tests.entity.ClusteredTweetId;
import java.util.Date;
import java.util.UUID;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Test;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update.Where;

/**
 * ClusteredEntityIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ClusteredEntityIT
{
    private Session session = CQLCassandraDaoTest.getCqlSession();

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    @Test
    public void should_persist_and_find() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        UUID tweetId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        Date creationDate = new Date();

        ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

        ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);

        em.persist(tweet);

        ClusteredTweet found = em.find(ClusteredTweet.class, id);

        assertThat(found.getContent()).isEqualTo("this is a tweet");
        assertThat(found.getOriginalAuthorId()).isEqualTo(userId);
        assertThat(found.getIsARetweet()).isFalse();
    }

    @Test
    public void should_merge() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        Long originalAuthorId = RandomUtils.nextLong();

        UUID tweetId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        Date creationDate = new Date();

        ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

        ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);
        tweet = em.merge(tweet);

        tweet.setContent("this is a new tweet2");
        tweet.setIsARetweet(true);
        tweet.setOriginalAuthorId(originalAuthorId);

        em.merge(tweet);

        ClusteredTweet found = em.find(ClusteredTweet.class, id);

        assertThat(found.getContent()).isEqualTo("this is a new tweet2");
        assertThat(found.getOriginalAuthorId()).isEqualTo(originalAuthorId);
        assertThat(found.getIsARetweet()).isTrue();
    }

    @Test
    public void should_remove() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        UUID tweetId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        Date creationDate = new Date();

        ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

        ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);

        tweet = em.merge(tweet);

        em.remove(tweet);

        ClusteredTweet found = em.find(ClusteredTweet.class, id);

        assertThat(found).isNull();
    }

    @Test
    public void should_refresh() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        Long originalAuthorId = RandomUtils.nextLong();
        UUID tweetId = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        Date creationDate = new Date();

        ClusteredTweetId id = new ClusteredTweetId(userId, tweetId, creationDate);

        ClusteredTweet tweet = new ClusteredTweet(id, "this is a tweet", userId, false);

        tweet = em.merge(tweet);

        Where update = QueryBuilder
                .update("clusteredtweet")
                .with(set("content", "New tweet"))
                .and(set("original_author_id", originalAuthorId))
                .and(set("is_a_retweet", true))
                .where(eq("user_id", userId))
                .and(eq("tweet_id", tweetId))
                .and(eq("creation_date", creationDate));

        session.execute(update);

        em.refresh(tweet);

        assertThat(tweet.getContent()).isEqualTo("New tweet");
        assertThat(tweet.getOriginalAuthorId()).isEqualTo(originalAuthorId);
        assertThat(tweet.getIsARetweet()).isTrue();
    }

    @Test
    public void should_persist_and_find_entity_having_compound_id_with_enum() throws Exception {
        long id = RandomUtils.nextLong();
        ClusteredMessageId messageId = new ClusteredMessageId(id, Type.TEXT);

        ClusteredMessage message = new ClusteredMessage(messageId, "a message");

        em.persist(message);

        ClusteredMessage found = em.find(ClusteredMessage.class, messageId);

        ClusteredMessageId foundCompoundKey = found.getId();
        assertThat(foundCompoundKey.getId()).isEqualTo(id);
        assertThat(foundCompoundKey.getType()).isEqualTo(Type.TEXT);
    }

    @Test
    public void should_merge_entity_having_compound_id_with_enum() throws Exception
    {
        long id = RandomUtils.nextLong();
        ClusteredMessageId messageId = new ClusteredMessageId(id, Type.IMAGE);

        ClusteredMessage message = new ClusteredMessage(messageId, "an image");

        message = em.merge(message);

        message.setLabel("a JPEG image");

        em.merge(message);

        ClusteredMessage found = em.find(ClusteredMessage.class, messageId);

        assertThat(found.getLabel()).isEqualTo("a JPEG image");
    }

    @Test
    public void should_remove_entity_having_compound_id_with_enum() throws Exception
    {
        long id = RandomUtils.nextLong();
        ClusteredMessageId messageId = new ClusteredMessageId(id, Type.AUDIO);

        ClusteredMessage message = new ClusteredMessage(messageId, "an mp3");

        message = em.merge(message);

        em.remove(message);

        ClusteredMessage found = em.find(ClusteredMessage.class, messageId);

        assertThat(found).isNull();
    }

    @Test
    public void should_refresh_entity_having_compound_id_with_enum() throws Exception
    {
        long id = RandomUtils.nextLong();
        ClusteredMessageId messageId = new ClusteredMessageId(id, Type.FILE);

        ClusteredMessage message = new ClusteredMessage(messageId, "a random file");

        message = em.merge(message);

        Where update = QueryBuilder
                .update("clusteredmessage")
                .with(set("label", "a pdf file"))
                .where(eq("id", id))
                .and(eq("type", "FILE"));

        session.execute(update);

        em.refresh(message);

        assertThat(message.getLabel()).isEqualTo("a pdf file");
    }

    @After
    public void tearDown()
    {
        truncateTables();
    }
}
