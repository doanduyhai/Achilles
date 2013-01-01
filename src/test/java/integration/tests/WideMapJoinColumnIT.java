package integration.tests;

import static fr.doan.achilles.columnFamily.ColumnFamilyHelper.normalizeCanonicalName;
import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getEntityDao;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;
import java.util.List;
import java.util.UUID;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.serializer.Utils;

/**
 * ThriftEntityManagerDirtyCheckIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapJoinColumnIT {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private final String ENTITY_PACKAGE = "integration.tests.entity";

    private GenericEntityDao<UUID> tweetDao = getEntityDao(Utils.UUID_SRZ,
            normalizeCanonicalName(Tweet.class.getCanonicalName()));

    private GenericEntityDao<Long> userDao = getEntityDao(Utils.LONG_SRZ,
            normalizeCanonicalName(User.class.getCanonicalName()));

    private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(getCluster(), getKeyspace(),
            ENTITY_PACKAGE, true);

    private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

    private Tweet ownTweet1;
    private Tweet ownTweet2;
    private Tweet ownTweet3;
    private Tweet ownTweet4;

    private Tweet fooTweet1;
    private Tweet fooTweet2;

    private Tweet barTweet1;
    private Tweet barTweet2;

    private User user;

    private Long userId = RandomUtils.nextLong();

    @Before
    public void setUp() {
        user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();

        ownTweet1 = TweetTestBuilder.tweet().randomId().content("myTweet1").creator(user).buid();
        ownTweet2 = TweetTestBuilder.tweet().randomId().content("myTweet2").creator(user).buid();
        ownTweet3 = TweetTestBuilder.tweet().randomId().content("myTweet3").creator(user).buid();
        ownTweet4 = TweetTestBuilder.tweet().randomId().content("myTweet4").creator(user).buid();

        fooTweet1 = TweetTestBuilder.tweet().randomId().content("friend1Tweet1").buid();
        fooTweet2 = TweetTestBuilder.tweet().randomId().content("friend1Tweet2").buid();

        barTweet1 = TweetTestBuilder.tweet().randomId().content("friend2Tweet1").buid();
        barTweet2 = TweetTestBuilder.tweet().randomId().content("friend2Tweet2").buid();

    }

    @Test
    public void should_insert_join_tweets() throws Exception {

        user = em.merge(user);

        user.getTweets().insert(1, ownTweet1);
        user.getTweets().insert(2, ownTweet2);

        DynamicComposite startComp = new DynamicComposite();
        startComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
        startComp.addComponent(1, "tweets", ComponentEquality.EQUAL);

        DynamicComposite endComp = new DynamicComposite();
        endComp.addComponent(0, JOIN_WIDE_MAP.flag(), ComponentEquality.EQUAL);
        endComp.addComponent(1, "tweets", ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<DynamicComposite, Object>> columns = userDao.findColumnsRange(user.getId(), startComp, endComp,
                false, 20);

        assertThat(columns).hasSize(2);

        assertThat(columns.get(0).right).isEqualTo(ownTweet1.getId());
        assertThat(columns.get(1).right).isEqualTo(ownTweet2.getId());

        Tweet foundOwnTweet1 = em.find(Tweet.class, ownTweet1.getId());
        Tweet foundOwnTweet2 = em.find(Tweet.class, ownTweet2.getId());

        assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
        assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
        assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet2.getId());
        assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet2.getContent());
    }

    @Test
    public void should_find_join_tweets() throws Exception {

        user = em.merge(user);

        user.getTweets().insert(1, ownTweet1);
        user.getTweets().insert(2, ownTweet2);

        List<KeyValue<Integer, Tweet>> foundOwnTweets = user.getTweets().findRange(2, 1, true, 5);

        assertThat(foundOwnTweets).hasSize(2);

        Tweet foundOwnTweet1 = foundOwnTweets.get(0).getValue();
        Tweet foundOwnTweet2 = foundOwnTweets.get(1).getValue();

        assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet2.getId());
        assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet2.getContent());
        assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet1.getId());
        assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet1.getContent());
    }

    @Test
    public void should_remove_join_tweets() throws Exception {

        user = em.merge(user);

        user.getTweets().insert(1, ownTweet1);
        user.getTweets().insert(2, ownTweet2);
        user.getTweets().insert(3, ownTweet3);
        user.getTweets().insert(4, ownTweet4);

        System.out.println("ownTweet1.getId = " + ownTweet1.getId());
        System.out.println("ownTweet2.getId = " + ownTweet2.getId());
        System.out.println("ownTweet3.getId = " + ownTweet3.getId());
        System.out.println("ownTweet4.getId = " + ownTweet4.getId());

        user.getTweets().removeRange(2, true, 4, false);

        List<KeyValue<Integer, Tweet>> foundOwnTweets = user.getTweets().findRange(1, 4, false, 10);

        assertThat(foundOwnTweets).hasSize(2);

        Tweet foundOwnTweet1 = foundOwnTweets.get(0).getValue();
        Tweet foundOwnTweet2 = foundOwnTweets.get(1).getValue();

        assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet1.getId());
        assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet1.getContent());
        assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet4.getId());
        assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet4.getContent());

        assertThat(em.find(Tweet.class, ownTweet1.getId())).isNotNull();
        assertThat(em.find(Tweet.class, ownTweet2.getId())).isNotNull();
        assertThat(em.find(Tweet.class, ownTweet3.getId())).isNotNull();
        assertThat(em.find(Tweet.class, ownTweet4.getId())).isNotNull();
    }

    @Test
    public void should_iterate_through_tweets() throws Exception {
        user = em.merge(user);

        user.getTweets().insert(1, ownTweet1);
        user.getTweets().insert(2, ownTweet2);
        user.getTweets().insert(3, ownTweet3);
        user.getTweets().insert(4, ownTweet4);

        KeyValueIterator<Integer, Tweet> iterator = user.getTweets().iterator(1, false, 3, true, false, 10);

        Tweet foundOwnTweet1 = iterator.next().getValue();
        Tweet foundOwnTweet2 = iterator.next().getValue();

        assertThat(foundOwnTweet1.getId()).isEqualTo(ownTweet2.getId());
        assertThat(foundOwnTweet1.getContent()).isEqualTo(ownTweet2.getContent());
        assertThat(foundOwnTweet2.getId()).isEqualTo(ownTweet3.getId());
        assertThat(foundOwnTweet2.getContent()).isEqualTo(ownTweet3.getContent());

    }

    @Test
    public void should_exception_when_inserting_tweets_which_do_not_exist_in_db() throws Exception {

    }

    @After
    public void tearDown() {
        tweetDao.truncate();
    }
}
