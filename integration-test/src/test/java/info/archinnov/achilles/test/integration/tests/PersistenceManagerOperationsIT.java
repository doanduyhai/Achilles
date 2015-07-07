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

import static info.archinnov.achilles.options.OptionsBuilder.withTimestamp;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.options.OptionsBuilder;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.type.CounterBuilder;

public class PersistenceManagerOperationsIT {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean");

    private PersistenceManager manager = resource.getPersistenceManager();
    private Session session = manager.getNativeSession();

    @Test
    public void should_persist() throws Exception {

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        manager.insert(entity);

        Row row = session.execute("select name,age_in_years,friends,followers,preferences from completebean where id = "
                + entity.getId()).one();

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

        row = session.execute(
                "select counter_value from achilles_counter_table where fqcn = '"
                        + CompleteBean.class.getCanonicalName() + "' and primary_key='" + entity.getId()
                        + "' and property_name='version'").one();

        assertThat(row.getLong("counter_value")).isEqualTo(15L);

    }

    @Test
    public void should_persist_empty_entity() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

        manager.insert(entity);

        CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found).isNotNull();
        assertThat(found.getId()).isEqualTo(entity.getId());
        assertThat(found.getFriends()).isNotNull().isEmpty();
        assertThat(found.getFollowers()).isNull();
        assertThat(found.getPreferences()).isNull();
    }

    @Test
    public void should_overwrite_existing_values_on_persist() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai")
                .addFriends("foo", "bar", "qux").addFollowers("John", "Helen").addPreference(1, "Paris")
                .addPreference(2, "Ile de France").addPreference(3, "FRANCE").buid();

        manager.insert(entity);

        entity.getFriends().clear();
        entity.getFollowers().clear();
        entity.getPreferences().clear();

        // Should clean collections & maps before persisting again
        manager.insert(entity);

        entity = manager.find(CompleteBean.class, entity.getId());

        assertThat(entity.getFriends()).isNotNull().isEmpty();
        assertThat(entity.getFollowers()).isNull();
        assertThat(entity.getPreferences()).isNull();

    }

    @Test
    public void should_insert_or_update() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();

        //When
        manager.insertOrUpdate(entity);

        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("DuyHai");

        //When
        found.setName("Paul");
        manager.insertOrUpdate(found);

        final CompleteBean updated = manager.find(CompleteBean.class, entity.getId());

        //Then
        assertThat(updated).isNotNull();
        assertThat(found.getName()).isEqualTo("Paul");

    }

    @Test
    public void should_find() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();

        manager.insert(entity);

        CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Jonathan");
    }

    @Test
    public void should_update_modifications() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(40L)
                .addFriends("bob", "alice").addFollowers("Billy", "Stephen", "Jacky").addPreference(1, "US")
                .addPreference(2, "New York").buid();
        manager.insert(entity);

        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.setAge(100L);
        proxy.getFriends().add("eve");
        proxy.getPreferences().put(1, "FR");

        manager.update(proxy);

        Row row = session.execute("select * from completebean where id=" + entity.getId()).one();

        assertThat(row.getLong("age_in_years")).isEqualTo(100L);
        assertThat(row.getList("friends", String.class)).containsExactly("bob", "alice", "eve");
        Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);
        assertThat(preferences.get(1)).isEqualTo("FR");
        assertThat(preferences.get(2)).isEqualTo("New York");
    }

    @Test
    public void should_delete_property_after_merge() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(40L)
                .addFriends("bob", "alice").addFollowers("Billy", "Stephen", "Jacky").addPreference(1, "US")
                .addPreference(2, "New York").buid();
        manager.insert(entity);

        CompleteBean found = manager.find(CompleteBean.class, entity.getId(),OptionsBuilder.withProxy());

        found.setName(null);
        found.setFriends(null);
        found.setFollowers(null);
        found.setPreferences(null);

        manager.update(found);

        found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found.getName()).isNull();
        assertThat(found.getFriends()).isNotNull().isEmpty();
        assertThat(found.getFollowers()).isNull();
        assertThat(found.getPreferences()).isNull();

    }

    @Test
    public void should_exception_when_trying_to_modify_primary_key() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(40L)
                .addFriends("bob", "alice").addFollowers("Billy", "Stephen", "Jacky").addPreference(1, "US")
                .addPreference(2, "New York").buid();

        manager.insert(entity);

        exception.expect(IllegalAccessException.class);
        exception.expectMessage("Cannot change primary key value for existing entity");

        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());
        proxy.setId(RandomUtils.nextLong(0, Long.MAX_VALUE));
    }

    @Test
    public void should_delete() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        manager.insert(entity);

        manager.delete(entity);

        assertThat(manager.find(CompleteBean.class, entity.getId())).isNull();

        List<Row> rows = session.execute("select * from completebean where id=" + entity.getId()).all();
        assertThat(rows).isEmpty();
    }

    @Test
    public void should_delete_by_id() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        manager.insert(entity);

        manager.deleteById(CompleteBean.class, entity.getId());

        assertThat(manager.find(CompleteBean.class, entity.getId())).isNull();

        List<Row> rows = session.execute("select * from completebean where id=" + entity.getId()).all();
        assertThat(rows).isEmpty();
    }

    @Test
    public void should_delete_transient_entity() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        manager.insert(entity);

        manager.delete(entity);

        assertThat(manager.find(CompleteBean.class, entity.getId())).isNull();
    }

    @Test
    public void should_get_proxy_for_update() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.setName("new name");
        proxy.setAge(36L);
        proxy.getFriends().add("qux");
        proxy.getFollowers().add("Richard");
        proxy.getPreferences().put(3, "75001");

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found.getName()).isEqualTo("new name");
        assertThat(found.getAge()).isEqualTo(36L);
        assertThat(found.getFriends()).containsExactly("foo", "bar", "qux");
        assertThat(found.getFollowers()).contains("George", "Paul", "Richard");

        assertThat(found.getPreferences()).containsKey(1);
        assertThat(found.getPreferences()).containsKey(2);
        assertThat(found.getPreferences()).containsKey(3);

        assertThat(found.getPreferences()).containsValue("FR");
        assertThat(found.getPreferences()).containsValue("Paris");
        assertThat(found.getPreferences()).containsValue("75001");
    }

    @Test
    public void should_get_proxy_for_list_update() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .addFriends("foo", "bar").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        final List<String> friends = proxy.getFriends();
        friends.add("qux"); // foo bar qux
        friends.add(0, "alice"); // alice foo bar qux
        friends.set(1, "bob"); // alice foo bob qux
        friends.addAll(Arrays.asList("Richard", "Paul")); // alice foo bob qux Richard Paul

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found.getFriends()).containsExactly("alice", "foo", "bob", "qux", "Richard", "Paul");
    }

    @Test
    @Ignore
    //FIXME Inconsistencies because list mutations are applied as batch so ordering is not guaranteed
    public void should_intercept_switching_list() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .addFriends("foo", "bar").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.setFriends(Arrays.asList("a","b","c"));
        final List<String> friends = proxy.getFriends();
        friends.add("qux"); // a b c qux
        friends.add(0, "alice"); // alice a b c qux
        friends.set(1, "bob"); // alice a bob c qux
        friends.addAll(Arrays.asList("Richard", "Paul")); // alice a bob c qux Richard Paul

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found.getFriends()).containsExactly("alice", "a", "bob", "c", "qux", "Richard", "Paul");
    }

    @Test
    public void should_get_proxy_for_set_update() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .addFollowers("foo", "bar").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        final Set<String> followers = proxy.getFollowers();
        followers.add("qux");
        followers.addAll(Arrays.asList("bob", "alice"));
        followers.remove("foo");
        followers.removeAll(Arrays.asList("foo", "bar"));

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found.getFollowers()).contains("alice", "bob", "qux");
    }

    @Test
    public void should_intercept_switching_set() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .addFollowers("foo", "bar").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.setFollowers(Sets.newHashSet("a", "c", "c"));
        final Set<String> followers = proxy.getFollowers();
        followers.add("qux");
        followers.addAll(Arrays.asList("bob", "alice"));
        followers.remove("foo");
        followers.remove("b");
        followers.removeAll(Arrays.asList("foo", "bar"));

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found.getFollowers()).contains("a", "c", "alice", "bob", "qux");
    }

    @Test
    public void should_get_proxy_for_map_update() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .addPreference(1, "Paris").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        final Map<Integer, String> preferences = proxy.getPreferences();
        preferences.put(1, "FR");
        preferences.putAll(ImmutableMap.of(2, "Paris", 3, "Rue de la Paix"));
        preferences.remove(2);

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        final Map<Integer, String> foundPreferences = found.getPreferences();

        assertThat(foundPreferences).hasSize(2);

        assertThat(foundPreferences).containsKey(1);
        assertThat(foundPreferences).doesNotContainKey(2);
        assertThat(foundPreferences).containsKey(3);

        assertThat(foundPreferences.get(1)).isEqualTo("FR");
        assertThat(foundPreferences.get(3)).isEqualTo("Rue de la Paix");
    }

    @Test
    public void should_intercept_switching_map() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
                .addPreference(1, "Paris").buid();

        manager.insert(entity);

        //When
        CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.setPreferences(ImmutableMap.of(10, "10", 11, "11"));

        final Map<Integer, String> preferences = proxy.getPreferences();
        preferences.put(1,"FR");
        preferences.putAll(ImmutableMap.of(2, "Paris", 3, "Rue de la Paix"));
        preferences.remove(2);
        preferences.remove(10);

        manager.update(proxy);

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        final Map<Integer, String> foundPreferences = found.getPreferences();

        assertThat(foundPreferences).hasSize(3);

        assertThat(foundPreferences).containsKey(1);
        assertThat(foundPreferences).doesNotContainKey(2);
        assertThat(foundPreferences).containsKey(3);
        assertThat(foundPreferences).doesNotContainKey(10);
        assertThat(foundPreferences).containsKey(11);

        assertThat(foundPreferences.get(1)).isEqualTo("FR");
        assertThat(foundPreferences.get(3)).isEqualTo("Rue de la Paix");
        assertThat(foundPreferences.get(11)).isEqualTo("11");
    }

    @Test
    public void should_find_unmapped_field() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai")
                .label("label").age(35L).addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        manager.insert(entity);

        assertThat(entity.getLabel()).isEqualTo("label");
    }

    @Test
    public void should_return_null_and_not_wrapper_for_null_values() throws Exception {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
                .buid();

        entity.setFriends(null);
        entity.setFollowers(null);
        entity.setPreferences(null);

        manager.insert(entity);

        entity = manager.find(CompleteBean.class, entity.getId());

        assertThat(entity.getFriends()).isNotNull().isInstanceOf(ArrayList.class).isEmpty();
        assertThat(entity.getFollowers()).isNull();
        assertThat(entity.getPreferences()).isNull();
        assertThat(entity.getLabel()).isNull();
        assertThat(entity.getAge()).isNull();
    }

    @Test
    public void should_return_empty_list_instead_of_null_for_field_annotated_with_emptyIfNullCollection() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final CompleteBean completeBean = new CompleteBean();
        completeBean.setId(id);

        manager.insert(completeBean);

        final CompleteBean found = manager.find(CompleteBean.class, id);

        //When
        final List<String> friends = found.getFriends();

        //Then
        assertThat(friends).isNotNull().isEmpty();
    }

    @Test
    public void should_delete_with_timestamp() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final CompleteBean entity = CompleteBeanTestBuilder.builder().id(id).name("John").buid();
        manager.insert(entity);

        //When
        manager.delete(entity, withTimestamp(1L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());

        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("John");

    }
}
