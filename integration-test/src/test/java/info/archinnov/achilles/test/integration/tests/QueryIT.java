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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.column;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ConsistencyLevel;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.test.integration.entity.*;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.*;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import net.sf.cglib.proxy.Factory;

public class QueryIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
            CompleteBean.class.getSimpleName(), TABLE_NAME, ClusteredEntityWithTimeUUID.TABLE_NAME,
            AchillesCounter.ACHILLES_COUNTER_TABLE);

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    @Test
    public void should_return_rows_for_native_query() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        CompleteBean entity2 = builder().randomId().name("John DOO").age(35L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(CounterBuilder.incr(17L)).buid();

        manager.insert(entity1);
        manager.insert(entity2);

        RegularStatement statement = select("name", "age_in_years", "friends", "followers", "preferences")
                .from("CompleteBean").where(in("id", entity1.getId(), entity2.getId()));

        List<TypedMap> actual = manager.nativeQuery(statement).get();

        assertThat(actual).hasSize(2);

        TypedMap row1 = actual.get(0);
        TypedMap row2 = actual.get(1);

        assertThat(row1.get("name")).isEqualTo("DuyHai");
        assertThat(row1.get("age_in_years")).isEqualTo(35L);
        assertThat(row1.<List<String>>getTyped("friends")).containsExactly("foo", "bar");
        assertThat(row1.<Set<String>>getTyped("followers")).contains("George", "Paul");
        Map<Integer, String> preferences1 = row1.getTyped("preferences");
        assertThat(preferences1.get(1)).isEqualTo("FR");
        assertThat(preferences1.get(2)).isEqualTo("Paris");
        assertThat(preferences1.get(3)).isEqualTo("75014");

        assertThat(row2.get("name")).isEqualTo("John DOO");
        assertThat(row2.get("age_in_years")).isEqualTo(35L);
        assertThat(row2.<List<String>>getTyped("friends")).containsExactly("qux", "twix");
        assertThat(row2.<Set<String>>getTyped("followers")).contains("Isaac", "Lara");
        Map<Integer, String> preferences2 = row2.getTyped("preferences");
        assertThat(preferences2.get(1)).isEqualTo("US");
        assertThat(preferences2.get(2)).isEqualTo("NewYork");
    }

    @Test
    public void should_return_rows_for_native_query_with_bound_values() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").buid();
        manager.insert(entity);

        RegularStatement statement = select("name").from("CompleteBean").where(eq("id", bindMarker()));

        List<TypedMap> actual = manager.nativeQuery(statement, entity.getId()).get();

        assertThat(actual).hasSize(1);

        TypedMap row = actual.get(0);

        assertThat(row.get("name")).isEqualTo("DuyHai");
    }

    @Test
    public void should_execute_native_query_with_LWT() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.insert(entity);

        final Insert statement = insertInto("CompleteBean").ifNotExists().value("id", bindMarker("id")).value("name", bindMarker("name"));

        final AtomicBoolean error = new AtomicBoolean(false);
        final AtomicReference<LWTResultListener.LWTResult> result = new AtomicReference<>(null);

        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {

            }

            @Override
            public void onError(LWTResult lwtResult) {
                error.getAndSet(true);
                result.getAndSet(lwtResult);
            }
        };

        //When
        manager.nativeQuery(statement,OptionsBuilder.lwtResultListener(listener),entity.getId(),"DuyHai").execute();

        //Then
        assertThat(error.get()).isTrue();
        assertThat(result.get()).isNotNull();

        final TypedMap currentValues = result.get().currentValues();

        assertThat(currentValues.<Long>getTyped("id")).isEqualTo(entity.getId());
        assertThat(currentValues.<String>getTyped("name")).isEqualTo(entity.getName());
        assertThat(currentValues.<String>getTyped("label")).isEqualTo(entity.getLabel());
    }

    @Test
    public void should_return_count_for_native_query() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").buid();

        manager.insert(entity);

        RegularStatement statement = select().countAll().from("CompleteBean").where(eq("id", entity.getId()));

        Long count = (Long) manager.nativeQuery(statement).getFirst().get("count");

        assertThat(count).isEqualTo(1L);
    }

    @Test
    public void should_return_ttl_and_timestamp_for_native_query() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").age(32L).buid();

        Long timestamp = (System.currentTimeMillis() + 1234500) * 1000;

        manager.insert(entity, OptionsBuilder.withTtl(1000).withTimestamp(timestamp));

        RegularStatement statement = select().fcall("ttl", column("name")).fcall("writetime",column("age_in_years"))
                .from("CompleteBean").where(eq("id",entity.getId()));

        Map<String, Object> result = manager.nativeQuery(statement).getFirst();

        assertThat((Integer) result.get("ttl(name)")).isLessThanOrEqualTo(1000);
        assertThat(result.get("writetime(age_in_years)")).isEqualTo(timestamp);
    }

    @Test
    public void should_return_cql_functions_for_native_query() throws Exception {

        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        UUID date = UUIDGen.getTimeUUID();

        manager.insert(new ClusteredEntityWithTimeUUID(id, date, "value"));

        RegularStatement statement = select()
                .fcall("now")
                .fcall("dateOf", column("date"))
                .fcall("unixTimestampOf", column("date"))
                .from(ClusteredEntityWithTimeUUID.TABLE_NAME)
                .where(eq("id", id));

        Map<String, Object> result = manager.nativeQuery(statement).getFirst();
        assertThat(result.get("now()")).isNotNull().isInstanceOf(UUID.class);
        assertThat(result.get("dateOf(date)")).isNotNull().isInstanceOf(Date.class);
        assertThat(result.get("unixTimestampOf(date)")).isNotNull().isInstanceOf(Long.class);
    }

    @Test
    public void should_return_iterator_for_native_query() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").buid();
        CompleteBean entity2 = builder().randomId().name("Paul").buid();
        CompleteBean entity3 = builder().randomId().name("George").buid();
        CompleteBean entity4 = builder().randomId().name("John").buid();
        CompleteBean entity5 = builder().randomId().name("Helen").buid();

        manager.insert(entity1);
        manager.insert(entity2);
        manager.insert(entity3);
        manager.insert(entity4);
        manager.insert(entity5);

        List<String> possibleNames = Arrays.asList("DuyHai", "Paul", "George", "John", "Helen");

        RegularStatement statement = select().all().from("CompleteBean").limit(6);
        statement.setFetchSize(2);

        final Iterator<TypedMap> iterator = manager.nativeQuery(statement).iterator();

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getTyped("name")).isIn(possibleNames);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getTyped("name")).isIn(possibleNames);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getTyped("name")).isIn(possibleNames);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getTyped("name")).isIn(possibleNames);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getTyped("name")).isIn(possibleNames);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_return_entities_for_typed_query_with_select_star() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        CompleteBean entity2 = builder().randomId().name("John DOO").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").buid();

        manager.insert(entity1);
        manager.insert(entity2);

        RegularStatement statement = select().from("CompleteBean").limit(3);

        List<CompleteBean> actual = manager.typedQuery(CompleteBean.class, statement).get();

        assertThat(actual).hasSize(2);

        CompleteBean found1 = actual.get(0);
        CompleteBean found2 = actual.get(1);

        Factory factory1 = (Factory) found1;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor1 = (ProxyInterceptor<CompleteBean>) factory1.getCallback(0);

        CompleteBean target1 = (CompleteBean) interceptor1.getTarget();

        assertThat(target1.getLabel()).isNull();
        assertThat(target1.getWelcomeTweet()).isNull();

        Factory factory2 = (Factory) found1;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor2 = (ProxyInterceptor<CompleteBean>) factory2.getCallback(0);

        CompleteBean target2 = (CompleteBean) interceptor2.getTarget();

        assertThat(target2.getLabel()).isNull();
        assertThat(target2.getWelcomeTweet()).isNull();

        if (found1.getId().equals(entity1.getId())) {
            CompleteBean reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isTrue();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getAge()).isEqualTo(reference.getAge());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());
            assertThat(found1.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found1.getPreferences().get(1)).isEqualTo("FR");
            assertThat(found1.getPreferences().get(2)).isEqualTo("Paris");
            assertThat(found1.getPreferences().get(3)).isEqualTo("75014");

            reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isTrue();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getAge()).isEqualTo(reference.getAge());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
            assertThat(found2.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found2.getPreferences().get(1)).isEqualTo("US");
            assertThat(found2.getPreferences().get(2)).isEqualTo("NewYork");
        } else {
            CompleteBean reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isTrue();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());
            assertThat(found1.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found1.getPreferences().get(1)).isEqualTo("US");
            assertThat(found1.getPreferences().get(2)).isEqualTo("NewYork");

            reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isTrue();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
            assertThat(found2.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found2.getPreferences().get(1)).isEqualTo("FR");
            assertThat(found2.getPreferences().get(2)).isEqualTo("Paris");
            assertThat(found2.getPreferences().get(3)).isEqualTo("75014");
        }
    }

    @Test
    public void should_return_entities_for_typed_query_with_simple_select() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        CompleteBean entity2 = builder().randomId().name("John DOO").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").buid();

        manager.insert(entity1);
        manager.insert(entity2);

        RegularStatement statement = select("id","name","friends").from("CompleteBean").limit(3);
        List<CompleteBean> actual = manager.typedQuery(CompleteBean.class, statement).get();

        assertThat(actual).hasSize(2);

        CompleteBean found1 = actual.get(0);
        CompleteBean found2 = actual.get(1);

        Factory factory1 = (Factory) found1;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor1 = (ProxyInterceptor<CompleteBean>) factory1.getCallback(0);

        CompleteBean target1 = (CompleteBean) interceptor1.getTarget();

        assertThat(target1.getAge()).isNull();
        assertThat(target1.getFollowers()).isNull();
        assertThat(target1.getLabel()).isNull();
        assertThat(target1.getPreferences()).isNull();
        assertThat(target1.getWelcomeTweet()).isNull();

        Factory factory2 = (Factory) found1;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor2 = (ProxyInterceptor<CompleteBean>) factory2.getCallback(0);

        CompleteBean target2 = (CompleteBean) interceptor2.getTarget();

        assertThat(target2.getAge()).isNull();
        assertThat(target2.getFollowers()).isNull();
        assertThat(target2.getLabel()).isNull();
        assertThat(target2.getPreferences()).isNull();
        assertThat(target2.getWelcomeTweet()).isNull();

        if (found1.getId().equals(entity1.getId())) {
            CompleteBean reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isTrue();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());

            reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isTrue();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
        } else {
            CompleteBean reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isTrue();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());

            reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isTrue();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
        }
    }

    @Test
    public void should_return_entity_for_typed_query_with_bound_values() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").buid();

        manager.insert(entity);

        RegularStatement statement = select("id","name","friends").from("CompleteBean").where(eq("id",bindMarker()));
        List<CompleteBean> actual = manager.typedQuery(CompleteBean.class, statement, entity.getId()).get();

        assertThat(actual).hasSize(1);

        CompleteBean found = actual.get(0);
        assertThat(found.getName()).isEqualTo(entity.getName());
    }

    @Test
    public void should_return_raw_entities_for_raw_typed_query_with_select_star() throws Exception {
        Counter counter1 = CounterBuilder.incr(15L);
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(counter1).buid();

        Counter counter2 = CounterBuilder.incr(17L);
        CompleteBean entity2 = builder().randomId().name("John DOO").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(counter2).buid();

        manager.insert(entity1);
        manager.insert(entity2);

        RegularStatement statement = select().from("CompleteBean").limit(bindMarker("lim"));
        List<CompleteBean> actual = manager.rawTypedQuery(CompleteBean.class, statement, 3).get();

        assertThat(actual).hasSize(2);

        CompleteBean found1 = actual.get(0);
        CompleteBean found2 = actual.get(1);

        if (found1.getId().equals(entity1.getId())) {
            CompleteBean reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isFalse();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getAge()).isEqualTo(reference.getAge());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());
            assertThat(found1.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found1.getPreferences().get(1)).isEqualTo("FR");
            assertThat(found1.getPreferences().get(2)).isEqualTo("Paris");
            assertThat(found1.getPreferences().get(3)).isEqualTo("75014");
            assertThat(found1.getVersion()).isNull();

            reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isFalse();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getAge()).isEqualTo(reference.getAge());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
            assertThat(found2.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found2.getPreferences().get(1)).isEqualTo("US");
            assertThat(found2.getPreferences().get(2)).isEqualTo("NewYork");
            assertThat(found2.getVersion()).isNull();
        } else {
            CompleteBean reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isFalse();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());
            assertThat(found1.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found1.getPreferences().get(1)).isEqualTo("US");
            assertThat(found1.getPreferences().get(2)).isEqualTo("NewYork");
            assertThat(found1.getVersion()).isNull();

            reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isFalse();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
            assertThat(found2.getFollowers()).containsAll(reference.getFollowers());
            assertThat(found2.getPreferences().get(1)).isEqualTo("FR");
            assertThat(found2.getPreferences().get(2)).isEqualTo("Paris");
            assertThat(found2.getPreferences().get(3)).isEqualTo("75014");
            assertThat(found2.getVersion()).isNull();
        }
    }

    @Test
    public void should_return_raw_entities_for_raw_typed_query_with_simple_select() throws Exception {
        Counter counter1 = CounterBuilder.incr(15L);
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(counter1).buid();

        Counter counter2 = CounterBuilder.incr(17L);
        CompleteBean entity2 = builder().randomId().name("John DOO").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(counter2).buid();

        manager.insert(entity1);
        manager.insert(entity2);

        RegularStatement statement = select("id","name","friends").from("CompleteBean").limit(3);
        List<CompleteBean> actual = manager.rawTypedQuery(CompleteBean.class, statement).get();

        assertThat(actual).hasSize(2);

        CompleteBean found1 = actual.get(0);
        CompleteBean found2 = actual.get(1);

        if (found1.getId().equals(entity1.getId())) {
            CompleteBean reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isFalse();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());
            assertThat(found1.getVersion()).isNull();

            reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isFalse();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
            assertThat(found2.getVersion()).isNull();
        } else {
            CompleteBean reference = entity2;

            assertThat(Factory.class.isAssignableFrom(found1.getClass())).isFalse();
            assertThat(found1.getId()).isEqualTo(reference.getId());
            assertThat(found1.getName()).isEqualTo(reference.getName());
            assertThat(found1.getFriends()).containsAll(reference.getFriends());
            assertThat(found1.getVersion()).isNull();

            reference = entity1;

            assertThat(Factory.class.isAssignableFrom(found2.getClass())).isFalse();
            assertThat(found2.getId()).isEqualTo(reference.getId());
            assertThat(found2.getName()).isEqualTo(reference.getName());
            assertThat(found2.getFriends()).containsAll(reference.getFriends());
            assertThat(found2.getVersion()).isNull();
        }
    }

    @Test
    public void should_return_raw_entity_for_raw_typed_query_with_bound_values() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").buid();
        manager.insert(entity);

        RegularStatement statement = select("name").from("CompleteBean").limit(bindMarker());
        List<CompleteBean> actual = manager.rawTypedQuery(CompleteBean.class, statement, 3).get();

        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getName()).isEqualTo(entity.getName());
    }

    @Test
    public void should_return_first_entity_for_typed_query_with_simple_select() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        manager.insert(entity);

        RegularStatement statement = select("id","name","friends").from("CompleteBean").limit(3);
        CompleteBean actual = manager.typedQuery(CompleteBean.class, statement).getFirst();

        Factory factory1 = (Factory) actual;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor1 = (ProxyInterceptor<CompleteBean>) factory1.getCallback(0);

        CompleteBean target1 = (CompleteBean) interceptor1.getTarget();

        assertThat(target1.getAge()).isNull();
        assertThat(target1.getFollowers()).isNull();
        assertThat(target1.getLabel()).isNull();
        assertThat(target1.getPreferences()).isNull();
        assertThat(target1.getWelcomeTweet()).isNull();

        assertThat(Factory.class.isAssignableFrom(actual.getClass())).isTrue();
        assertThat(actual.getId()).isEqualTo(entity.getId());
        assertThat(actual.getName()).isEqualTo(entity.getName());
        assertThat(actual.getFriends()).containsAll(entity.getFriends());
        assertThat(actual.getVersion().get()).isEqualTo(15L);

    }

    @Test
    public void should_return_first_clustered_entity_for_typed_query_with_select_star() throws Exception {
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);

        ClusteredEntity entity = new ClusteredEntity(id, 10, "name", "value");
        manager.insert(entity);

        RegularStatement statement = select().from(TABLE_NAME).limit(3);
        ClusteredEntity actual = manager.typedQuery(ClusteredEntity.class, statement).getFirst();

        assertThat(actual).isNotNull();
        assertThat(actual).isInstanceOf(Factory.class);

        ClusteredEntity.CompoundPK compoundPK = actual.getId();

        assertThat(compoundPK).isNotNull();
        assertThat(compoundPK.getId()).isEqualTo(id);
        assertThat(compoundPK.getCount()).isEqualTo(10);
        assertThat(compoundPK.getName()).isEqualTo("name");
    }

    @Test
    public void should_return_first_raw_entity_for_raw_typed_query_with_simple_select() throws Exception {
        CompleteBean entity = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        manager.insert(entity);

        RegularStatement statement = select("id","name","friends").from("CompleteBean").limit(3);
        CompleteBean actual = manager.rawTypedQuery(CompleteBean.class, statement).getFirst();

        assertThat(Factory.class.isAssignableFrom(actual.getClass())).isFalse();

        assertThat(actual.getId()).isEqualTo(entity.getId());
        assertThat(actual.getName()).isEqualTo(entity.getName());
        assertThat(actual.getLabel()).isNull();
        assertThat(actual.getAge()).isNull();
        assertThat(actual.getFriends()).containsAll(entity.getFriends());
        assertThat(actual.getFollowers()).isNull();
        assertThat(actual.getPreferences()).isNull();
        assertThat(actual.getVersion()).isNull();
        assertThat(actual.getWelcomeTweet()).isNull();

    }

    @Test
    public void should_return_first_raw_clustered_entity_for_raw_query_with_simple_select() throws Exception {
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);

        ClusteredEntity entity = new ClusteredEntity(id, 10, "name", "value");
        manager.insert(entity);

        RegularStatement statement = select("id","count","name","value").from(TABLE_NAME).limit(3);
        ClusteredEntity actual = manager.rawTypedQuery(ClusteredEntity.class, statement).getFirst();

        assertThat(actual).isNotNull();

        ClusteredEntity.CompoundPK compoundPK = actual.getId();

        assertThat(compoundPK).isNotNull();
        assertThat(compoundPK.getId()).isEqualTo(id);
        assertThat(compoundPK.getCount()).isEqualTo(10);
        assertThat(compoundPK.getName()).isEqualTo("name");
    }

    @Test
    public void should_ignore_null_varargs_for_bounded_values() {
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.insert(entity);

        final Select.Where select = select().from("CompleteBean").where(QueryBuilder.eq("id", entity.getId()));
        final TypedQuery<CompleteBean> queryBuilder = manager.typedQuery(CompleteBean.class, select, (Object[])null);

        // When
        final CompleteBean actual = queryBuilder.getFirst();

        // Then
        assertThat(actual.getLabel()).isEqualTo("label");
    }

    @Test
    public void should_apply_null_heap_byte_buffer() {
        // Given
        Tweet entity = TweetTestBuilder.tweet().randomId().content("label").buid();

        manager.insert(entity);

        final Select.Where select = select().from("Tweet").where(QueryBuilder.eq("id", entity.getId()));
        final ByteBuffer[] values = select.getValues(ProtocolVersion.V2);
        final TypedQuery<Tweet> queryBuilder = manager.typedQuery(Tweet.class, select, new Object[]{values});

        // When
        final Tweet actual = queryBuilder.getFirst();

        // Then
        assertThat(actual).isNotNull();
    }

    @Test
    public void should_apply_null_bounded_values() {
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.insert(entity);

        final Select.Where select = select().from("CompleteBean").where(QueryBuilder.eq("id", entity.getId()));
        final TypedQuery<CompleteBean> queryBuilder = manager.typedQuery(CompleteBean.class, select, (Object[])null);

        // When
        final CompleteBean actual = queryBuilder.getFirst();

        // Then
        assertThat(actual.getLabel()).isEqualTo("label");
    }

    @Test
    public void should_allow_native_and_typed_query_with_bound_statement() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final Session session = manager.getNativeSession();
        final PreparedStatement insertPs = session.prepare(insertInto(CompleteBean.TABLE_NAME)
                .value("id", bindMarker("id"))
                .value("label", bindMarker("label"))
                .value("age_in_years", bindMarker("age")));
        final BoundStatement insertBs = insertPs.bind(id, "label", 32L);
        manager.nativeQuery(insertBs).execute();

        final PreparedStatement selectPs = session.prepare(select().from(CompleteBean.TABLE_NAME).where(eq("id", bindMarker("id"))));
        final BoundStatement selectBs = selectPs.bind(id);

        //When
        final CompleteBean foundWithProxy = manager.typedQuery(CompleteBean.class, selectBs).getFirst();
        final CompleteBean foundRaw = manager.rawTypedQuery(CompleteBean.class, selectBs).getFirst();

        //Then
        assertThat(foundWithProxy).isNotNull();
        assertThat(foundWithProxy.getLabel()).isEqualTo("label");
        assertThat(foundWithProxy.getAge()).isEqualTo(32L);

        assertThat(foundRaw).isNotNull();
        assertThat(foundRaw.getLabel()).isEqualTo("label");
        assertThat(foundRaw.getAge()).isEqualTo(32L);
    }

    @Test
    public void should_allow_regular_statement_with_consistency_level() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final CompleteBean entity = builder().id(id).label("label123").buid();

        manager.insert(entity);

        final Select.Where regularStatement = select().column("id").column("label").from(CompleteBean.TABLE_NAME)
                .where(eq("id",bindMarker("id")));
        regularStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        final CompleteBean found = manager.typedQuery(CompleteBean.class, regularStatement, id).getFirst();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getLabel()).isEqualTo("label123");
        logAsserter.assertConsistencyLevels(info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM);
    }
}
