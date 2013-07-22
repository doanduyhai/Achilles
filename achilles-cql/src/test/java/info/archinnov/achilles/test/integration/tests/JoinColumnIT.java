package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import net.sf.cglib.proxy.Factory;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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

    private Session session = CQLCassandraDaoTest.getCqlSession();

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    private Tweet tweet;
    private User creator;
    private Long creatorId = RandomUtils.nextLong();

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

        Row row = session.execute("select * from Tweet where id=" + tweet.getId()).one();

        assertThat(row.getString("content")).isEqualTo("this is a tweet");
        assertThat(row.getLong("creator")).isEqualTo(creatorId);
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
        tweet = em.unwrap(tweet);

        assertThat(tweet).isNotInstanceOf(Factory.class);
        assertThat(tweet.getCreator()).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_persist_user_and_referrer() throws Exception
    {

        creator = UserTestBuilder.user().randomId().firstname("creator").buid();
        User referrer = UserTestBuilder.user().randomId().firstname("referrer").buid();

        creator.setReferrer(referrer);
        referrer.setReferrer(creator);

        em.persist(creator);

        User foundCreator = em.find(User.class, creator.getId());

        User foundReferrer = foundCreator.getReferrer();
        assertThat(foundReferrer.getId()).isEqualTo(referrer.getId());
        assertThat(foundReferrer.getFirstname()).isEqualTo("referrer");
    }

    @Test
    public void should_merge_user_and_referrer() throws Exception
    {

        creator = UserTestBuilder.user().randomId().firstname("creator").buid();
        User referrer = UserTestBuilder.user().randomId().firstname("referrer").buid();

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

        assertThat(modifiedCreator.getFirstname()).isEqualTo("modified_creator");
        assertThat(modifiedReferrer.getFirstname()).isEqualTo("modified_referrer");
    }

    @Test
    public void should_persist_transient_user_and_referrer_with_merge() throws Exception
    {

        creator = UserTestBuilder.user().randomId().firstname("creator").buid();
        User referrer = UserTestBuilder.user().randomId().firstname("referrer").buid();

        creator.setReferrer(referrer);
        referrer.setReferrer(creator);

        em.merge(creator);

        User foundCreator = em.find(User.class, creator.getId());

        User foundReferrer = foundCreator.getReferrer();
        assertThat(foundReferrer.getId()).isEqualTo(referrer.getId());
        assertThat(foundReferrer.getFirstname()).isEqualTo("referrer");
    }

    @After
    public void tearDown()
    {
        truncateTable("CompleteBean");
        truncateTable("Tweet");
    }
}
