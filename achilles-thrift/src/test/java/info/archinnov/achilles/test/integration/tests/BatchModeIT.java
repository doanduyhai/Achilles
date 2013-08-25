package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftBatchingFlushContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.embedded.ThriftEmbeddedServer;
import info.archinnov.achilles.entity.manager.ThriftBatchingEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import java.util.Map;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

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

    @Rule
    public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(Steps.AFTER_TEST,
            "CompleteBean", "User", "Tweet");

    private ThriftEntityManagerFactory emf = resource.getFactory();

    private ThriftEntityManager em = resource.getEm();

    private ThriftCounterDao thriftCounterDao = resource.getCounterDao();

    private ThriftConsistencyLevelPolicy policy = resource.getConsistencyPolicy();

    private ThriftGenericEntityDao completeBeanDao = resource.getEntityDao(
            normalizerAndValidateColumnFamilyName(CompleteBean.class.getCanonicalName()),
            Long.class);

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    private User user;

    private Long userId = RandomUtils.nextLong();

    @Before
    public void setUp()
    {
        user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();
    }

    @Test
    public void should_batch_counters() throws Exception
    {
        // Start batch
        ThriftBatchingEntityManager batchEm = emf.createBatchingEntityManager();
        batchEm.startBatch();

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

        entity = batchEm.merge(entity);

        entity.setLabel("label");

        Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("welcomeTweet").buid();
        entity.setWelcomeTweet(welcomeTweet);

        entity.getVersion().incr(10L);
        batchEm.merge(entity);

        Composite labelComposite = new Composite();
        labelComposite.addComponent(0, LAZY_SIMPLE.flag(), EQUAL);
        labelComposite.addComponent(1, "label", EQUAL);
        labelComposite.addComponent(2, 0, EQUAL);

        assertThat(completeBeanDao.getValue(entity.getId(), labelComposite)).isNull();

        Composite counterKey = createCounterKey(CompleteBean.class, entity.getId());
        Composite versionCounterName = createCounterName("version");

        assertThat(thriftCounterDao.getCounterValue(counterKey, versionCounterName)).isEqualTo(10L);

        // Flush
        batchEm.endBatch();

        assertThat(completeBeanDao.getValue(entity.getId(), labelComposite)).isEqualTo("label");

        assertThat(thriftCounterDao.getCounterValue(counterKey, versionCounterName)).isEqualTo(10L);
        assertThatBatchContextHasBeenReset(batchEm);
    }

    @Test
    public void should_batch_several_entities() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();

        // Start batch
        ThriftBatchingEntityManager batchEm = emf.createBatchingEntityManager();
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
        assertThatConsistencyLevelHasBeenReset();
    }

    @Test
    public void should_reinit_batch_context_after_exception() throws Exception
    {
        User user = UserTestBuilder.user().id(123456494L).firstname("firstname")
                .lastname("lastname").buid();
        Tweet tweet = TweetTestBuilder.tweet().randomId().content("simple_tweet").creator(user)
                .buid();

        // Start batch
        ThriftBatchingEntityManager batchEm = emf.createBatchingEntityManager();
        batchEm.startBatch();

        try
        {
            batchEm.persist(tweet);
        } catch (AchillesException e)
        {
            batchEm.cleanBatch();
            assertThatBatchContextHasBeenReset(batchEm);
            assertThatConsistencyLevelHasBeenReset();

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
    public void should_batch_with_custom_consistency_level() throws Exception
    {
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();
        Tweet tweet3 = TweetTestBuilder.tweet().randomId().content("simple_tweet3").buid();

        em.persist(tweet1);

        // Start batch
        ThriftBatchingEntityManager batchEm = emf.createBatchingEntityManager();
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
        assertThatConsistencyLevelHasBeenReset();
    }

    @Test
    public void should_reinit_batch_context_and_consistency_after_exception() throws Exception
    {
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();

        em.persist(tweet1);

        // Start batch
        ThriftBatchingEntityManager batchEm = emf.createBatchingEntityManager();
        batchEm.startBatch();

        batchEm.startBatch(EACH_QUORUM);
        batchEm.persist(tweet2);

        try
        {
            batchEm.endBatch();
        } catch (Exception e)
        {
            assertThatBatchContextHasBeenReset(batchEm);
            assertThatConsistencyLevelHasBeenReset();
        }

        Thread.sleep(1000);
        logAsserter.prepareLogLevel();
        batchEm.persist(tweet2);
        logAsserter.assertConsistencyLevels(QUORUM, QUORUM);
        assertThatConsistencyLevelHasBeenReset();
    }

    private void assertThatConsistencyLevelHasBeenReset()
    {
        assertThat(policy.getCurrentReadLevel()).isNull();
        assertThat(policy.getCurrentWriteLevel()).isNull();
    }

    private void assertThatBatchContextHasBeenReset(ThriftBatchingEntityManager batchEm)
    {
        ThriftBatchingFlushContext flushContext = Whitebox
                .getInternalState(batchEm, "flushContext");
        Map<String, Pair<Mutator<?>, ThriftAbstractDao>> mutatorMap = Whitebox.getInternalState(
                flushContext, "mutatorMap");
        boolean hasCustomConsistencyLevels = (Boolean) Whitebox.getInternalState(flushContext,
                "hasCustomConsistencyLevels");

        assertThat(mutatorMap).isEmpty();
        assertThat(hasCustomConsistencyLevels).isFalse();

    }

    private <T> Composite createCounterKey(Class<T> clazz, Long id)
    {
        Composite comp = new Composite();
        comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
        comp.setComponent(1, id.toString(), STRING_SRZ);
        return comp;
    }

    private Composite createCounterName(String propertyName)
    {
        Composite composite = new Composite();
        composite.addComponent(0, propertyName, ComponentEquality.EQUAL);
        return composite;
    }

    @AfterClass
    public static void cleanUp()
    {
        ThriftEmbeddedServer.policy().reinitCurrentConsistencyLevels();
        ThriftEmbeddedServer.policy().reinitDefaultConsistencyLevels();
    }
}
