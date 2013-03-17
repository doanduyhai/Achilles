package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getDynamicCompositeDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.serializer.SerializerUtils;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;

import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * BatchModeIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class BatchModeIT
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

	private User user;

	private Long userId = RandomUtils.nextLong();

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();

		ownTweet1 = TweetTestBuilder.tweet().randomId().content("myTweet1").creator(user).buid();
		ownTweet2 = TweetTestBuilder.tweet().randomId().content("myTweet2").creator(user).buid();

	}

	@Test
	public void should_insert_join_tweets_with_batch() throws Exception
	{

		user = em.merge(user);

		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "tweets", ComponentEquality.EQUAL);

		DynamicComposite endComp = new DynamicComposite();
		endComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
		endComp.addComponent(1, "tweets", ComponentEquality.GREATER_THAN_EQUAL);

		// Start batch
		em.startBatch(user);

		user.getTweets().insert(1, ownTweet1);
		user.getTweets().insert(2, ownTweet2);

		List<Pair<DynamicComposite, String>> columns = userDao.findColumnsRange(user.getId(),
				startComp, endComp, false, 20);

		Tweet foundOwnTweet1 = em.find(Tweet.class, ownTweet1.getId());
		Tweet foundOwnTweet2 = em.find(Tweet.class, ownTweet2.getId());

		assertThat(columns).hasSize(0);
		assertThat(foundOwnTweet1).isNull();
		assertThat(foundOwnTweet2).isNull();

		// End batch
		em.endBatch(user);

		columns = userDao.findColumnsRange(user.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);

		assertThat(readUUID(columns.get(0).right)).isEqualTo(ownTweet1.getId());
		assertThat(readUUID(columns.get(1).right)).isEqualTo(ownTweet2.getId());

		foundOwnTweet1 = em.find(Tweet.class, ownTweet1.getId());
		foundOwnTweet2 = em.find(Tweet.class, ownTweet2.getId());

		assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
		assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
		assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet2.getId());
		assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet2.getContent());

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
