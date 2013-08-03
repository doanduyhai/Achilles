package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import net.sf.cglib.proxy.Factory;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * JPAOperationsIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JPAOperationsIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Session session = CQLCassandraDaoTest.getCqlSession();

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

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

        Row row = session.execute(
                "select name,age_in_years,friends,followers,preferences from completebean where id = "
                        + bean.getId()).one();

        assertThat(row.getString("name")).isEqualTo("DuyHai");
        assertThat(row.getLong("age_in_years")).isEqualTo(35L);
        assertThat(row.getList("friends", String.class)).containsExactly("foo", "bar");
        assertThat(row.getSet("followers", String.class)).containsOnly("George", "Paul");

        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).containsKey(1);
        assertThat(preferences).containsKey(2);
        assertThat(preferences).containsKey(3);

        assertThat(preferences).containsValue("FR");
        assertThat(preferences).containsValue("Paris");
        assertThat(preferences).containsValue("75014");

        row = session.execute("select counter_value from achilles_counter_table where fqcn = '"
                + CompleteBean.class.getCanonicalName() + "' and primary_key='" + bean.getId()
                + "' and property_name='version'").one();

        assertThat(row.getLong("counter_value")).isEqualTo(15L);

    }

    @Test
    public void should_persist_empty_bean() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

        em.persist(bean);

        CompleteBean found = em.find(CompleteBean.class, bean.getId());

        assertThat(found).isNotNull();
        assertThat(found.getId()).isEqualTo(bean.getId());
    }

    @Test
    public void should_cascade_persist_with_ttl() throws Exception
    {
        Tweet tweet = new Tweet();
        tweet.setContent("this is a welcome tweet");
        tweet.setId(TimeUUIDUtils.getUniqueTimeUUIDinMillis());

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();
        entity.setWelcomeTweet(tweet);

        // Persist entity with ttl = 2 secs
        em.persist(entity, 2);

        assertThat(em.find(CompleteBean.class, entity.getId())).isNotNull();

        Thread.sleep(3000);

        assertThat(em.find(CompleteBean.class, entity.getId())).isNull();

        Tweet foundTweet = em.find(Tweet.class, tweet.getId());

        assertThat(foundTweet).isNotNull();
        assertThat(foundTweet.getContent()).isEqualTo(tweet.getContent());
    }

    @Test
    public void should_overwrite_existing_values_on_persist() throws Exception
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

        CompleteBean persistedBean = em.find(CompleteBean.class, bean.getId());

        assertThat(persistedBean).isNotNull();
        assertThat(persistedBean.getName()).isEqualTo("DuyHai");
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
        assertThat(entity.getFavoriteTweets()).isNull();

    }

    @Test
    public void should_cascade_merge_with_ttl() throws Exception
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
        em.merge(entity, 2);

        Thread.sleep(3000);

        CompleteBean foundEntity = em.find(CompleteBean.class, entity.getId());

        assertThat(foundEntity.getName()).isNull();

        Tweet foundTweet = em.find(Tweet.class, tweet.getId());

        assertThat(foundTweet.getContent()).isEqualTo("modified welcomed tweet");
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
        CQLEntityInterceptor<CompleteBean> interceptor = (CQLEntityInterceptor<CompleteBean>) factory
                .getCallback(0);

        Method getLabel = CompleteBean.class.getDeclaredMethod("getLabel");
        String label = (String) getLabel.invoke(interceptor.getTarget());

        assertThat(label).isNull();

        String lazyLabel = found.getLabel();

        assertThat(lazyLabel).isNotNull();
        assertThat(lazyLabel).isEqualTo("label");
    }

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
        CQLEntityInterceptor<CompleteBean> interceptor = (CQLEntityInterceptor<CompleteBean>) factory
                .getCallback(0);

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

        Row row = session.execute("select * from completebean where id=" + bean.getId()).one();

        assertThat(row.getLong("age_in_years")).isEqualTo(100L);
        assertThat(row.getList("friends", String.class)).containsExactly("bob", "alice", "eve");
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("New York");

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

        assertThat(em.find(CompleteBean.class, bean.getId())).isNull();

        List<Row> rows = session
                .execute("select * from completebean where id=" + bean.getId())
                .all();
        assertThat(rows).isEmpty();
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

        assertThat(em.find(CompleteBean.class, bean.getId())).isNull();

        List<Row> rows = session
                .execute("select * from completebean where id=" + bean.getId())
                .all();
        assertThat(rows).isEmpty();
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

        PropertyMeta<Void, String> nameMeta = new PropertyMeta<Void, String>();
        nameMeta.setType(PropertyType.SIMPLE);

        nameMeta.setPropertyName("name");

        session.execute("UPDATE completebean SET name='DuyHai_modified' WHERE id=" + bean.getId());
        session.execute("UPDATE completebean SET friends=friends + ['qux'] WHERE id="
                + bean.getId());

        em.refresh(bean);

        assertThat(bean.getName()).isEqualTo("DuyHai_modified");
        assertThat(bean.getFriends()).hasSize(3);
        assertThat(bean.getFriends().get(2)).isEqualTo("qux");
    }

    @Test(expected = AchillesStaleObjectStateException.class)
    public void should_exception_when_staled_object_during_refresh() throws Exception
    {

        CompleteBean bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .buid();

        bean = em.merge(bean);

        session.execute("DELETE FROM completebean WHERE id=" + bean.getId());

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

    @After
    public void tearDown()
    {
        truncateTable("CompleteBean");
    }
}
