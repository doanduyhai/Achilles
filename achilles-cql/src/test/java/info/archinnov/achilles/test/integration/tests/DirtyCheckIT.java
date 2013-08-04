package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * DirtyCheckIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class DirtyCheckIT
{

    private Session session = CQLCassandraDaoTest.getCqlSession();

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();
    private CompleteBean bean;

    @Before
    public void setUp()
    {
        bean = CompleteBeanTestBuilder.builder().randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        bean = em.merge(bean);
    }

    @Test
    public void should_dirty_check_list_element_add() throws Exception
    {
        bean.getFriends().add("qux");

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(3);
        assertThat(friends.get(2)).isEqualTo("qux");
    }

    @Test
    public void should_dirty_check_list_element_add_at_index() throws Exception
    {
        bean.getFriends().add(1, "qux");

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();

        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(3);
        assertThat(friends.get(1)).isEqualTo("qux");
        assertThat(friends.get(2)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_add_all() throws Exception
    {
        bean.getFriends().addAll(Arrays.asList("qux", "baz"));

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(4);
        assertThat(friends.get(2)).isEqualTo("qux");
        assertThat(friends.get(3)).isEqualTo("baz");
    }

    @Test
    public void should_dirty_check_list_element_clear() throws Exception
    {
        bean.getFriends().clear();

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        assertThat(row.isNull("friends")).isTrue();
    }

    @Test
    public void should_dirty_check_list_element_remove_at_index() throws Exception
    {
        bean.getFriends().remove(0);

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_remove_element() throws Exception
    {
        bean.getFriends().remove("bar");

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("foo");
    }

    @Test
    public void should_dirty_check_list_element_remove_all() throws Exception
    {
        bean.getFriends().removeAll(Arrays.asList("foo", "qux"));

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_retain_all() throws Exception
    {
        bean.getFriends().retainAll(Arrays.asList("foo", "qux"));

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("foo");
    }

    @Test
    public void should_dirty_check_list_element_sub_list_remove() throws Exception
    {
        bean.getFriends().subList(0, 1).remove(0);

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_set() throws Exception
    {
        bean.getFriends().set(1, "qux");

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends.get(1)).isEqualTo("qux");
    }

    @Test
    public void should_dirty_check_list_element_iterator_remove() throws Exception
    {
        Iterator<String> iter = bean.getFriends().iterator();

        iter.next();
        iter.remove();

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_list_iterator_remove() throws Exception
    {
        Iterator<String> iter = bean.getFriends().listIterator();

        iter.next();
        iter.remove();

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo("bar");
    }

    @Test
    public void should_dirty_check_list_element_list_iterator_set() throws Exception
    {
        ListIterator<String> iter = bean.getFriends().listIterator();

        iter.next();
        iter.set("qux");

        em.merge(bean);

        Row row = session.execute("select friends from CompleteBean where id=" + bean.getId()).one();
        List<String> friends = row.getList("friends", String.class);

        assertThat(friends).hasSize(2);
        assertThat(friends.get(0)).isEqualTo("qux");
    }

    @Test
    public void should_dirty_check_map_put_element() throws Exception
    {
        bean.getPreferences().put(4, "test");

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(4);
        assertThat(preferences.get(4)).isEqualTo("test");
    }

    @Test
    public void should_dirty_check_map_remove_key() throws Exception
    {
        bean.getPreferences().remove(1);

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_map_put_all() throws Exception
    {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(3, "75015");
        map.put(4, "test");
        bean.getPreferences().putAll(map);

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(4);
        assertThat(preferences.get(3)).isEqualTo("75015");
        assertThat(preferences.get(4)).isEqualTo("test");

    }

    @Test
    public void should_dirty_check_map_keyset_remove() throws Exception
    {
        bean.getPreferences().keySet().remove(1);

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_dirty_check_map_keyset_remove_all() throws Exception
    {
        bean.getPreferences().keySet().removeAll(Arrays.asList(1, 2, 5));

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(1);
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_dirty_check_map_keyset_retain_all() throws Exception
    {
        bean.getPreferences().keySet()
                .retainAll(Arrays.asList(1, 3));

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_dirty_check_map_keyset_iterator_remove() throws Exception
    {
        Iterator<Integer> iter = bean.getPreferences().keySet().iterator();

        iter.next();
        iter.remove();

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_dirty_check_map_valueset_remove() throws Exception
    {
        bean.getPreferences().values().remove("FR");

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_map_valueset_remove_all() throws Exception
    {
        bean.getPreferences().values().removeAll(Arrays.asList("FR", "Paris", "test"));

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(1);
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_map_valueset_retain_all() throws Exception
    {
        bean.getPreferences().values().retainAll(Arrays.asList("FR", "Paris", "test"));

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("Paris");
    }

    @Test
    public void should_dirty_check_map_valueset_iterator_remove() throws Exception
    {
        Iterator<String> iter = bean.getPreferences().values().iterator();

        iter.next();
        iter.remove();

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_dirty_check_map_entrySet_remove_entry() throws Exception
    {

        Set<Entry<Integer, String>> entrySet = bean.getPreferences().entrySet();

        Entry<Integer, String> entry = entrySet.iterator().next();

        entrySet.remove(entry);

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(2);
        assertThat(preferences.get(2)).isEqualTo("Paris");
        assertThat(preferences.get(3)).isEqualTo("75014");

    }

    @Test
    public void should_dirty_check_map_entrySet_remove_all_entry() throws Exception
    {

        Set<Entry<Integer, String>> entrySet = bean.getPreferences().entrySet();

        Iterator<Entry<Integer, String>> iterator = entrySet.iterator();

        Entry<Integer, String> entry1 = iterator.next();
        Entry<Integer, String> entry2 = iterator.next();

        entrySet.removeAll(Arrays.asList(entry1, entry2));

        em.merge(bean);

        Row row = session.execute("select preferences from CompleteBean where id=" + bean.getId()).one();
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

        assertThat(preferences).hasSize(1);
        assertThat(preferences.get(3)).isEqualTo("75014");
    }

    @Test
    public void should_dirty_check_simple_property() throws Exception
    {
        bean.setName("another_name");

        em.merge(bean);

        Row row = session.execute("select name from CompleteBean where id=" + bean.getId()).one();
        Object reloadedName = row.getString("name");

        assertThat(reloadedName).isEqualTo("another_name");
    }

    @Test
    public void should_dirty_check_lazy_simple_property() throws Exception
    {
        bean.setLabel("label");

        em.merge(bean);

        Row row = session.execute("select label from CompleteBean where id=" + bean.getId()).one();
        Object reloadedLabel = row.getString("label");

        assertThat(reloadedLabel).isEqualTo("label");
    }

    @Test
    public void should_dirty_check_lazy_simple_property_after_loading() throws Exception
    {
        assertThat(bean.getLabel()).isNull();

        bean.setLabel("label");

        em.merge(bean);

        Row row = session.execute("select label from CompleteBean where id=" + bean.getId()).one();
        Object reloadedLabel = row.getString("label");

        assertThat(reloadedLabel).isEqualTo("label");
    }

    @Test
    public void should_cascade_dirty_check_join_simple_property() throws Exception
    {
        Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("Welcome").buid();

        CompleteBean myBean = CompleteBeanTestBuilder.builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .addFollowers("George", "Paul")
                .addPreference(1, "FR")
                .addPreference(2, "Paris")
                .addPreference(3, "75014")
                .buid();

        myBean.setWelcomeTweet(welcomeTweet);

        myBean = em.merge(myBean);

        Tweet welcomeTweetFromBean = myBean.getWelcomeTweet();
        welcomeTweetFromBean.setContent("new_welcome_message");

        em.merge(myBean);

        Tweet persistedWelcomeTweet = em.find(Tweet.class, welcomeTweet.getId());

        assertThat(persistedWelcomeTweet).isNotNull();
        assertThat(persistedWelcomeTweet.getContent()).isEqualTo("new_welcome_message");

    }

    @After
    public void tearDown()
    {
        truncateTable("CompleteBean");
    }
}
