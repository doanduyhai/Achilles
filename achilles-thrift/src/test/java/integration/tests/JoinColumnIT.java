package integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getEntityDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.table.TableHelper.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.Pair;
import integration.tests.entity.Tweet;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;

import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.TweetTestBuilder;

/**
 * JoinColumnIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinColumnIT
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private ThriftGenericEntityDao tweetDao = getEntityDao(
			normalizerAndValidateColumnFamilyName(Tweet.class.getCanonicalName()), UUID.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private Tweet tweet;
	private User creator;
	private Long creatorId = RandomUtils.nextLong();

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		creator = UserTestBuilder.user().id(creatorId).firstname("fn").lastname("ln").buid();
	}

	@Test
	public void should_persist_user_and_then_tweet() throws Exception
	{

		em.persist(creator);

		tweet = TweetTestBuilder
				.tweet()
				.randomId()
				.content("this is a tweet")
				.creator(creator)
				.buid();

		em.persist(tweet);

		Composite startComp = new Composite();
		startComp.addComponent(0, JOIN_SIMPLE.flag(), ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, JOIN_SIMPLE.flag(), ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = tweetDao.findColumnsRange(tweet.getId(), startComp,
				endComp, false, 20);

		assertThat(columns).hasSize(1);

		Pair<Composite, String> creator = columns.get(0);
		assertThat(readLong(creator.right)).isEqualTo(creatorId);

	}

	@Test
	public void should_find_user_from_tweet_after_persist() throws Exception
	{

		em.persist(creator);

		tweet = TweetTestBuilder
				.tweet()
				.randomId()
				.content("this is a tweet")
				.creator(creator)
				.buid();

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
	public void should_find_user_unchanged_from_tweet_after_merge() throws Exception
	{

		em.persist(creator);

		tweet = TweetTestBuilder
				.tweet()
				.randomId()
				.content("this is a tweet")
				.creator(creator)
				.buid();

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
	public void should_find_user_modified_from_tweet_after_refresh() throws Exception
	{

		em.persist(creator);

		tweet = TweetTestBuilder
				.tweet()
				.randomId()
				.content("this is a tweet")
				.creator(creator)
				.buid();

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
			throws Exception
	{

		creator = UserTestBuilder.user().id(RandomUtils.nextLong()).buid();
		tweet = TweetTestBuilder
				.tweet()
				.randomId()
				.content("this is a tweet")
				.creator(creator)
				.buid();

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
	public void should_unproxy_entity() throws Exception
	{
		em.persist(creator);

		tweet = TweetTestBuilder
				.tweet()
				.randomId()
				.content("this is a tweet")
				.creator(creator)
				.buid();

		tweet = em.merge(tweet);
		em.initialize(tweet);
		tweet = em.unproxy(tweet);

		assertThat(tweet).isNotInstanceOf(Factory.class);
		assertThat(tweet.getCreator()).isNotInstanceOf(Factory.class);
	}

	private Long readLong(String value) throws Exception
	{
		return this.objectMapper.readValue(value, Long.class);
	}

	@After
	public void tearDown()
	{
		tweetDao.truncate();
	}
}
