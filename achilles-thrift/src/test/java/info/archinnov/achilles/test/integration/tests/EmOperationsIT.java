package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.junit.AchillesThriftInternalResource;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.OptionsBuilder;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.Factory;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * EmOperationsIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class EmOperationsIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesThriftInternalResource resource = new AchillesThriftInternalResource(Steps.AFTER_TEST,
            "CompleteBean", "Tweet", "User", "achillesCounterCF");

    private ThriftEntityManager em = resource.getEm();

    private ThriftGenericEntityDao dao = resource.getEntityDao(
            normalizerAndValidateColumnFamilyName(CompleteBean.class.getName()), Long.class);

    private ThriftCounterDao counterDao = resource.getCounterDao();

    private ThriftCompositeFactory thriftCompositeFactory = new ThriftCompositeFactory();

    private ObjectMapper objectMapper = new ObjectMapper();

    private byte[] START_EAGER = new byte[]
    {
            0
    };
    private byte[] END_EAGER = new byte[]
    {
            20
    };

    @Test
    public void should_persist() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .version(CounterBuilder.incr(15L))
                .buid();

        em.persist(bean);

        Composite startCompositeForEagerFetch = new Composite();
        startCompositeForEagerFetch.addComponent(0, START_EAGER, ComponentEquality.EQUAL);

        Composite endCompositeForEagerFetch = new Composite();
        endCompositeForEagerFetch.addComponent(0, END_EAGER, ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(),
                startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

        assertThat(columns).hasSize(8);

        Pair<Composite, String> primaryKey = columns.get(0);

        Pair<Composite, String> age = columns.get(1);

        Pair<Composite, String> George = columns.get(2);
        Pair<Composite, String> Paul = columns.get(3);

        Pair<Composite, String> name = columns.get(4);

        Pair<Composite, String> FR = columns.get(5);
        Pair<Composite, String> Paris = columns.get(6);
        Pair<Composite, String> _75014 = columns.get(7);

        assertThat(primaryKey.left.get(1, STRING_SRZ)).isEqualTo("id");
        assertThat(Long.parseLong(primaryKey.right)).isEqualTo(bean.getId());

        assertThat(age.left.get(1, STRING_SRZ)).isEqualTo("age_in_years");
        assertThat(readLong(age.right)).isEqualTo(35L);

        assertThat(name.left.get(1, STRING_SRZ)).isEqualTo("name");
        assertThat(name.right).isEqualTo("DuyHai");

        assertThat(George.left.get(1, STRING_SRZ)).isEqualTo("followers");
        assertThat(George.right).isIn("George", "Paul");
        assertThat(Paul.left.get(1, STRING_SRZ)).isEqualTo("followers");
        assertThat(Paul.right).isIn("George", "Paul");

        assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
        KeyValue<Integer, String> country = readKeyValue(FR.right);
        assertThat(country.getKey()).isEqualTo(1);
        assertThat(country.getValue()).isEqualTo("FR");

        assertThat(Paris.left.get(1, STRING_SRZ)).isEqualTo("preferences");
        KeyValue<Integer, String> city = readKeyValue(Paris.right);
        assertThat(city.getKey()).isEqualTo(2);
        assertThat(city.getValue()).isEqualTo("Paris");

        assertThat(_75014.left.get(1, STRING_SRZ)).isEqualTo("preferences");
        KeyValue<Integer, String> zipCode = readKeyValue(_75014.right);
        assertThat(zipCode.getKey()).isEqualTo(3);
        assertThat(zipCode.getValue()).isEqualTo("75014");

        startCompositeForEagerFetch = new Composite();
        startCompositeForEagerFetch.addComponent(0, LAZY_LIST.flag(), ComponentEquality.EQUAL);
        startCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);

        endCompositeForEagerFetch = new Composite();
        endCompositeForEagerFetch.addComponent(0, LAZY_LIST.flag(), ComponentEquality.EQUAL);
        endCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.GREATER_THAN_EQUAL);

        columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
                endCompositeForEagerFetch, false, 20);
        assertThat(columns).hasSize(2);

        Pair<Composite, String> foo = columns.get(0);
        Pair<Composite, String> bar = columns.get(1);

        assertThat(foo.left.get(1, STRING_SRZ)).isEqualTo("friends");
        assertThat(foo.right).isEqualTo("foo");
        assertThat(bar.left.get(1, STRING_SRZ)).isEqualTo("friends");
        assertThat(bar.right).isEqualTo("bar");

        Composite counterRowKey = new Composite();
        counterRowKey.setComponent(0, CompleteBean.class.getCanonicalName(), STRING_SRZ);
        counterRowKey.setComponent(1, bean.getId().toString(), STRING_SRZ);

        Composite counterName = new Composite();
        counterName.addComponent(0, "version", ComponentEquality.EQUAL);

        Long version = counterDao.getCounterValue(counterRowKey, counterName);
        assertThat(version).isEqualTo(15L);
    }

    @Test
    public void should_persist_empty_bean() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        assertThat(found).isNotNull();
    }

    @Test
    public void should_cascade_persist_bi_directional_join() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        Long referrerId = RandomUtils.nextLong();
        User referrer = UserTestBuilder.user().id(referrerId).firstname("ref_fn").lastname("ref_ln").buid();
        User user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();

        user.setReferrer(referrer);
        referrer.setReferree(user);

        em.persist(user);

        assertThat(em.find(User.class, userId)).isNotNull();
        assertThat(em.find(User.class, referrerId)).isNotNull();
    }

    @Test
    public void should_cascade_persist_without_ttl() throws Exception
    {
        Tweet tweet = new Tweet();
        tweet.setContent("this is a welcome tweet");
        tweet.setId(TimeUUIDUtils.getUniqueTimeUUIDinMillis());

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();
        entity.setWelcomeTweet(tweet);

        // Persist entity with ttl = 2 secs
        em.persist(entity, OptionsBuilder.withTtl(2));

        Thread.sleep(3000);

        assertThat(em.find(CompleteBean.class, entity.getId())).isNull();

        Tweet foundTweet = em.find(Tweet.class, tweet.getId());

        assertThat(foundTweet).isNotNull();
        assertThat(foundTweet.getContent()).isEqualTo(tweet.getContent());
    }

    @Test
    public void should_cascade_persist_without_timestamp() throws Exception
    {
        Long id = RandomUtils.nextLong();
        UUID uuid = UUIDGen.getTimeUUID();
        Long timestamp1 = System.currentTimeMillis() * 1000;
        Long timestamp2 = timestamp1 + 100000000;

        Tweet tweet1 = new Tweet();
        tweet1.setContent("this is a welcome tweet 1");
        tweet1.setId(uuid);
        CompleteBean entity1 = CompleteBeanTestBuilder.builder().id(id).name("name1").buid();
        entity1.setWelcomeTweet(tweet1);

        Tweet tweet2 = new Tweet();
        tweet2.setContent("this is a welcome tweet 2");
        tweet2.setId(uuid);
        CompleteBean entity2 = CompleteBeanTestBuilder.builder().id(id).name("name2").buid();
        entity2.setWelcomeTweet(tweet2);

        // Persist entity2 with timestamp2
        em.persist(entity2, OptionsBuilder.withTimestamp(timestamp2));

        // Persist entity1 with timestamp1
        em.persist(entity1, OptionsBuilder.withTimestamp(timestamp1));

        CompleteBean foundEntity = em.find(CompleteBean.class, id);
        assertThat(foundEntity.getName()).isEqualTo("name2");

        Tweet foundTweet = em.find(Tweet.class, uuid);
        assertThat(foundTweet.getContent()).isEqualTo("this is a welcome tweet 1");
    }

    @Test
    public void should_clean_collections_and_maps_before_persist() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder()
                .randomId()
                .name("DuyHai")
                .addFriends("foo", "bar", "qux")
                .addFollowers("John", "Helen")
                .addPreference(1, "Paris")
                .addPreference(2, "Ile de France")
                .addPreference(3, "FRANCE")
                .buid();

        em.persist(entity);

        entity.getFriends().clear();
        entity.getFollowers().clear();
        entity.getPreferences().clear();

        // Should clean collections & maps before persisting again
        em.persist(entity);

        entity = em.find(CompleteBean.class, entity.getId());

        assertThat(entity.getFriends()).isNull();
        assertThat(entity.getFollowers()).isNull();
        assertThat(entity.getPreferences()).isNull();

    }

    @Test
    public void should_cascade_merge_join_simple_property() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
        Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("Welcome").buid();

        bean.setWelcomeTweet(welcomeTweet);

        em.merge(bean);

        Tweet persistedWelcomeTweet = em.find(Tweet.class, welcomeTweet.getId());

        assertThat(persistedWelcomeTweet).isNotNull();
        assertThat(persistedWelcomeTweet.getContent()).isEqualTo("Welcome");
    }

    @Test
    public void should_remove_join_collection_on_merge() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();

        Tweet tweet = TweetTestBuilder
                .tweet()
                .randomId()
                .content("This tweet should be put in favorite")
                .buid();
        entity.setFavoriteTweets(Arrays.asList(tweet));

        em.persist(entity);

        entity = em.find(CompleteBean.class, entity.getId());
        entity.getFavoriteTweets().clear();

        em.merge(entity);

        entity = em.find(CompleteBean.class, entity.getId());
        assertThat(entity.getFavoriteTweets()).isEmpty();

    }

    @Test
    public void should_cascade_merge_bi_directional_join() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        Long referrerId = RandomUtils.nextLong();
        User referrer = UserTestBuilder.user().id(referrerId).firstname("ref_fn").lastname("ref_ln").buid();
        User user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();

        user.setReferrer(referrer);
        referrer.setReferree(user);

        em.merge(user);

        assertThat(em.find(User.class, userId)).isNotNull();
        assertThat(em.find(User.class, referrerId)).isNotNull();
    }

    @Test
    public void should_cascade_merge_without_ttl() throws Exception
    {
        Tweet tweet = new Tweet();
        tweet.setContent("this is a welcome tweet");
        tweet.setId(TimeUUIDUtils.getUniqueTimeUUIDinMillis());

        CompleteBean entity = CompleteBeanTestBuilder.builder()
                .randomId()
                .name("DuyHai")
                .buid();
        entity.setWelcomeTweet(tweet);

        em.persist(entity);

        entity = em.find(CompleteBean.class, entity.getId());

        entity.getWelcomeTweet().setContent("modified welcomed tweet");
        entity.setName("DuyHai2");

        // Merge with ttl = 2 secs
        em.merge(entity, OptionsBuilder.withTtl(2));

        Thread.sleep(3000);

        CompleteBean foundEntity = em.find(CompleteBean.class, entity.getId());

        assertThat(foundEntity.getName()).isNull();

        Tweet foundTweet = em.find(Tweet.class, tweet.getId());

        assertThat(foundTweet.getContent()).isEqualTo("modified welcomed tweet");
    }

    @Test
    public void should_cascade_merge_without_timestamp() throws Exception
    {

        Long id = RandomUtils.nextLong();
        UUID uuid = UUIDGen.getTimeUUID();
        Long timestamp0 = (System.currentTimeMillis()) * 1000 + 100000000;
        Long timestamp1 = timestamp0 + 1000;
        Long timestamp2 = timestamp1 + 100000000;

        Tweet tweet = new Tweet();
        tweet.setContent("this is a welcome tweet");
        tweet.setId(uuid);

        CompleteBean entity = CompleteBeanTestBuilder.builder()
                .id(id)
                .name("DuyHai")
                .buid();
        entity.setWelcomeTweet(tweet);

        em.persist(entity, OptionsBuilder.withTimestamp(timestamp0));

        entity = em.find(CompleteBean.class, entity.getId());

        entity.getWelcomeTweet().setContent("modified welcomed tweet 2");
        entity.setName("DuyHai2");

        // Merge with timestamp2
        em.merge(entity, OptionsBuilder.withTimestamp(timestamp2));

        entity.getWelcomeTweet().setContent("modified welcomed tweet 1");
        entity.setName("DuyHai1");

        // Merge with timestamp1
        em.merge(entity, OptionsBuilder.withTimestamp(timestamp1));

        CompleteBean foundEntity = em.find(CompleteBean.class, entity.getId());

        assertThat(foundEntity.getName()).isEqualTo("DuyHai2");

        Tweet foundTweet = em.find(Tweet.class, tweet.getId());

        assertThat(foundTweet.getContent()).isEqualTo("modified welcomed tweet 1");
    }

    @Test
    public void should_find() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();

        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        assertThat(found).isNotNull();
        assertThat(found).isInstanceOf(Factory.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void should_find_lazy_simple() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("Jonathan")
                .label("label")
                .buid();

        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        Factory factory = (Factory) found;
        ThriftEntityInterceptor interceptor = (ThriftEntityInterceptor) factory.getCallback(0);

        Method getLabel = CompleteBean.class.getDeclaredMethod("getLabel");
        String label = (String) getLabel.invoke(interceptor.getTarget());

        assertThat(label).isNull();

        String lazyLabel = found.getLabel();

        assertThat(lazyLabel).isNotNull();
        assertThat(lazyLabel).isEqualTo("label");
    }

    @SuppressWarnings(
    {
            "rawtypes",
            "unchecked"
    })
    @Test
    public void should_find_lazy_list() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("Jonathan")
                .age(40L)
                .addFriends("bob", "alice")
                .addFollowers("Billy", "Stephen", "Jacky")
                .addPreference(1, "US")
                .addPreference(2, "New York")
                .buid();

        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        Factory factory = (Factory) found;
        ThriftEntityInterceptor interceptor = (ThriftEntityInterceptor) factory.getCallback(0);

        Method getFriends = CompleteBean.class.getDeclaredMethod("getFriends", (Class<?>[]) null);
        List<String> lazyFriends = (List<String>) getFriends.invoke(interceptor.getTarget());

        assertThat(lazyFriends).isNull();

        List<String> friends = found.getFriends();

        assertThat(friends).isNotNull();
        assertThat(friends).hasSize(2);
        assertThat(friends).containsExactly("bob", "alice");
    }

    @Test
    public void should_merge_modifications() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("Jonathan")
                .age(40L)
                .addFriends("bob", "alice")
                .addFollowers("Billy", "Stephen", "Jacky")
                .addPreference(1, "US")
                .addPreference(2, "New York")
                .buid();
        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        found.setAge(100L);
        found.getFriends().add("eve");
        found.getPreferences().put(1, "FR");

        CompleteBean merged = em.merge(found);

        assertThat(merged).isSameAs(found);

        assertThat(merged.getFriends()).hasSize(3);
        assertThat(merged.getFriends()).containsExactly("bob", "alice", "eve");
        assertThat(merged.getPreferences()).hasSize(2);
        assertThat(merged.getPreferences().get(1)).isEqualTo("FR");

        Composite composite = new Composite();
        composite.addComponent(0, PropertyType.SIMPLE.flag(), ComponentEquality.EQUAL);
        composite.addComponent(1, "age_in_years", ComponentEquality.EQUAL);
        composite.addComponent(2, 0, ComponentEquality.EQUAL);

        assertThat(readLong(dao.<Long, String> getValue(bean.getId(), composite))).isEqualTo(100L);

        Composite startCompositeForEagerFetch = new Composite();
        startCompositeForEagerFetch.addComponent(0, PropertyType.LAZY_LIST.flag(),
                ComponentEquality.EQUAL);
        startCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);
        startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

        Composite endCompositeForEagerFetch = new Composite();
        endCompositeForEagerFetch.addComponent(0, PropertyType.LAZY_LIST.flag(),
                ComponentEquality.EQUAL);
        endCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);
        endCompositeForEagerFetch.addComponent(2, 2, ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(),
                startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

        assertThat(columns).hasSize(3);

        Pair<Composite, String> eve = columns.get(2);

        assertThat(eve.left.get(1, STRING_SRZ)).isEqualTo("friends");
        assertThat(eve.right).isEqualTo("eve");

        startCompositeForEagerFetch = new Composite();
        startCompositeForEagerFetch.addComponent(0, PropertyType.MAP.flag(),
                ComponentEquality.EQUAL);
        startCompositeForEagerFetch.addComponent(1, "preferences", ComponentEquality.EQUAL);
        startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

        endCompositeForEagerFetch = new Composite();
        endCompositeForEagerFetch.addComponent(0, PropertyType.MAP.flag(), ComponentEquality.EQUAL);
        endCompositeForEagerFetch.addComponent(1, "preferences", ComponentEquality.EQUAL);
        endCompositeForEagerFetch.addComponent(2, 2, ComponentEquality.GREATER_THAN_EQUAL);

        columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
                endCompositeForEagerFetch, false, 20);

        assertThat(columns).hasSize(2);

        Pair<Composite, String> FR = columns.get(0);

        assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
        KeyValue<Integer, String> mapValue = readKeyValue(FR.right);
        assertThat(mapValue.getValue()).isEqualTo("FR");
    }

    @Test
    public void should_remove_property_after_merge() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("Jonathan")
                .age(40L)
                .addFriends("bob", "alice")
                .addFollowers("Billy", "Stephen", "Jacky")
                .addPreference(1, "US")
                .addPreference(2, "New York")
                .buid();
        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        found.setName(null);
        found.setFriends(null);
        found.setFollowers(null);
        found.setPreferences(null);

        em.merge(found);

        found = em.find(CompleteBean.class, bean.getId());

        assertThat(found.getName()).isNull();
        assertThat(found.getFriends()).isNull();
        assertThat(found.getFollowers()).isNull();
        assertThat(found.getPreferences()).isNull();

    }

    @Test
    public void should_exception_when_trying_to_modify_primary_key() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("Jonathan")
                .age(40L)
                .addFriends("bob", "alice")
                .addFollowers("Billy", "Stephen", "Jacky")
                .addPreference(1, "US")
                .addPreference(2, "New York")
                .buid();

        bean = em.merge(bean);

        exception.expect(IllegalAccessException.class);
        exception.expectMessage("Cannot change primary key value for existing entity");

        bean.setId(RandomUtils.nextLong());
    }

    @Test
    public void should_return_managed_entity_after_merge() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();
        bean = em.merge(bean);

        assertThat(bean).isInstanceOf(Factory.class);
    }

    @Test
    public void should_return_same_entity_as_merged_bean_when_managed() throws Exception
    {

        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
        Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
        bean.setWelcomeTweet(tweet);

        bean = em.merge(bean);

        CompleteBean bean2 = em.merge(bean);

        assertThat(bean2).isSameAs(bean);
        assertThat(bean.getWelcomeTweet()).isInstanceOf(Factory.class);
        assertThat(bean2.getWelcomeTweet()).isInstanceOf(Factory.class);
    }

    @Test
    public void should_remove() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        bean = em.merge(bean);

        em.remove(bean);

        CompleteBean foundBean = em.find(CompleteBean.class, bean.getId());

        assertThat(foundBean).isNull();

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), null, null,
                false, 20);

        assertThat(columns).hasSize(0);

    }

    @Test
    public void should_remove_by_id() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        bean = em.merge(bean);

        em.removeById(CompleteBean.class, bean.getId());

        CompleteBean foundBean = em.find(CompleteBean.class, bean.getId());

        assertThat(foundBean).isNull();

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), null, null,
                false, 20);

        assertThat(columns).hasSize(0);

    }

    @Test(expected = IllegalStateException.class)
    public void should_exception_when_removing_transient_entity() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        em.remove(bean);
    }

    @Test
    public void should_get_reference() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        em.persist(bean);

        CompleteBean foundBean = em.getReference(CompleteBean.class, bean.getId());

        assertThat(foundBean).isNotNull();

        // Real object should be empty
        CompleteBean realObject = em.unwrap(foundBean);

        assertThat(realObject.getId()).isEqualTo(bean.getId());
        assertThat(realObject.getName()).isNull();
        assertThat(realObject.getAge()).isNull();
        assertThat(realObject.getFriends()).isNull();
        assertThat(realObject.getFollowers()).isNull();
        assertThat(realObject.getPreferences()).isNull();

        assertThat(foundBean.getId()).isEqualTo(bean.getId());
        assertThat(foundBean.getName()).isEqualTo("DuyHai");
        assertThat(foundBean.getAge()).isEqualTo(35L);
        assertThat(foundBean.getFriends()).containsExactly("foo", "bar");
        assertThat(foundBean.getFollowers()).contains("George", "Paul");

        assertThat(foundBean.getPreferences()).containsKey(1);
        assertThat(foundBean.getPreferences()).containsKey(2);
        assertThat(foundBean.getPreferences()).containsKey(3);

        assertThat(foundBean.getPreferences()).containsValue("FR");
        assertThat(foundBean.getPreferences()).containsValue("Paris");
        assertThat(foundBean.getPreferences()).containsValue("75014");
    }

    @Test
    public void should_exception_refreshing_non_managed_entity() throws Exception
    {
        CompleteBean completeBean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("name")
                .buid();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("The entity '" + completeBean + "' is not in 'managed' state");
        em.refresh(completeBean);
    }

    @Test
    public void should_refresh() throws Exception
    {

        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        bean = em.merge(bean);

        bean.getFriends();

        PropertyMeta nameMeta = new PropertyMeta();
        nameMeta.setType(PropertyType.SIMPLE);

        nameMeta.setPropertyName("name");

        Composite nameComposite = thriftCompositeFactory.createForBatchInsertSingleValue(nameMeta);
        dao.setValue(bean.getId(), nameComposite, "DuyHai_modified");

        PropertyMeta listLazyMeta = new PropertyMeta();
        listLazyMeta.setType(LAZY_LIST);
        listLazyMeta.setPropertyName("friends");

        Composite friend3Composite = thriftCompositeFactory.createForBatchInsertMultiValue(
                listLazyMeta, 2);
        dao.setValue(bean.getId(), friend3Composite, "qux");

        em.refresh(bean);

        assertThat(bean.getName()).isEqualTo("DuyHai_modified");
        assertThat(bean.getFriends()).hasSize(3);
        assertThat(bean.getFriends().get(2)).isEqualTo("qux");

    }

    @Test(expected = RuntimeException.class)
    public void should_exception_when_staled_object_during_refresh() throws Exception
    {

        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .buid();

        bean = em.merge(bean);

        Mutator<Object> mutator = dao.buildMutator();
        dao.removeRowBatch(bean.getId(), mutator);
        dao.executeMutator(mutator);

        em.refresh(bean);

    }

    @Test
    public void should_find_unmapped_field() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
                .label("label")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        bean = em.merge(bean);

        assertThat(bean.getLabel()).isEqualTo("label");

    }

    @Test
    public void should_return_null_and_not_wrapper_for_null_values() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
                .buid();

        bean.setFriends(null);
        bean.setFollowers(null);
        bean.setPreferences(null);

        em.persist(bean);

        bean = em.find(CompleteBean.class, bean.getId());

        assertThat(bean.getFriends()).isNull();
        assertThat(bean.getFollowers()).isNull();
        assertThat(bean.getPreferences()).isNull();
        assertThat(bean.getLabel()).isNull();
        assertThat(bean.getAge()).isNull();
    }

    @Test
    public void should_not_exception_when_loading_column_family_with_unmapped_property()
            throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
                .buid();

        em.persist(bean);

        Composite composite = new Composite();
        composite.addComponent(0, SIMPLE.flag(), ComponentEquality.EQUAL);
        composite.addComponent(1, "unmappedProperty", ComponentEquality.EQUAL);
        composite.addComponent(2, 0, ComponentEquality.EQUAL);

        dao.setValue(bean.getId(), composite, "this is an unmapped property");

        bean = em.find(CompleteBean.class, bean.getId());

        assertThat(bean.getName()).isEqualTo("DuyHai");

    }

    private Long readLong(String value) throws Exception
    {
        return this.objectMapper.readValue(value, Long.class);
    }

    @SuppressWarnings("unchecked")
    private KeyValue<Integer, String> readKeyValue(String value) throws Exception
    {
        return this.objectMapper.readValue(value, KeyValue.class);
    }
}
