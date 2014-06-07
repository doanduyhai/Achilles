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
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTimeUUID;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;
import info.archinnov.achilles.type.TypedMap;
import net.sf.cglib.proxy.Factory;

public class AsyncQueryIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
            CompleteBean.class.getSimpleName(), TABLE_NAME, ClusteredEntityWithTimeUUID.TABLE_NAME,
            AchillesCounter.ACHILLES_COUNTER_TABLE);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_return_rows_for_native_query_async() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        CompleteBean entity2 = builder().randomId().name("John DOO").age(35L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(CounterBuilder.incr(17L)).buid();

        manager.insert(entity1);
        manager.insert(entity2);


        final RegularStatement statement = select("name","age_in_years","friends","followers","preferences")
                .from("CompleteBean").where(eq("id", bindMarker("id")));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Object> successSpy = new AtomicReference<>();
        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        final AchillesFuture<List<TypedMap>> future1 = manager.nativeQuery(statement, entity1.getId()).asyncGet(successCallBack);
        final AchillesFuture<List<TypedMap>> future2 = manager.nativeQuery(statement, entity2.getId()).asyncGet();

        final List<TypedMap> typedMaps1 = future1.get();
        assertThat(typedMaps1).hasSize(1);
        TypedMap typedMap1 = typedMaps1.get(0);

        final List<TypedMap> typedMaps2 = future2.get();
        assertThat(typedMaps2).hasSize(1);
        TypedMap typedMap2 = typedMaps2.get(0);

        assertThat(typedMap1.get("name")).isEqualTo("DuyHai");
        assertThat(typedMap1.get("age_in_years")).isEqualTo(35L);
        assertThat(typedMap1.<List<String>>getTyped("friends")).containsExactly("foo", "bar");
        assertThat(typedMap1.<Set<String>>getTyped("followers")).contains("George", "Paul");
        Map<Integer, String> preferences1 = typedMap1.getTyped("preferences");
        assertThat(preferences1.get(1)).isEqualTo("FR");
        assertThat(preferences1.get(2)).isEqualTo("Paris");
        assertThat(preferences1.get(3)).isEqualTo("75014");

        assertThat(typedMap2.get("name")).isEqualTo("John DOO");
        assertThat(typedMap2.get("age_in_years")).isEqualTo(35L);
        assertThat(typedMap2.<List<String>>getTyped("friends")).containsExactly("qux", "twix");
        assertThat(typedMap2.<Set<String>>getTyped("followers")).contains("Isaac", "Lara");
        Map<Integer, String> preferences2 = typedMap2.getTyped("preferences");
        assertThat(preferences2.get(1)).isEqualTo("US");
        assertThat(preferences2.get(2)).isEqualTo("NewYork");

        latch.await();
        assertThat(successSpy.get()).isNotNull().isInstanceOf(List.class);
    }

    @Test
    public void should_excecute_DML_native_query_with_async_listeners() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<Object> successSpy = new AtomicReference<>();
        final AtomicReference<Throwable> exceptionSpy = new AtomicReference<>();

        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        FutureCallback<Object> exceptionCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                exceptionSpy.getAndSet(t);
                latch.countDown();
            }
        };

        final RegularStatement insert = insertInto("completebean").value("id",bindMarker("id"));
        final RegularStatement delete = delete().from("completebean").where(eq("name","test"));

        //When
        manager.nativeQuery(insert, id).asyncExecute(successCallBack);
        manager.nativeQuery(delete).asyncExecute(exceptionCallBack);

        latch.await();
        Thread.sleep(100);

        //Then
        assertThat(successSpy.get()).isNotNull().isSameAs(Empty.INSTANCE);
        assertThat(exceptionSpy.get()).isNotNull().isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void should_return_async_iterator_for_native_query() throws Exception {
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

        final AchillesFuture<Iterator<TypedMap>> futureIterator = manager.nativeQuery(statement).asyncIterator();

        while (!futureIterator.isDone()) {
            Thread.sleep(10);
        }

        final Iterator<TypedMap> iterator = futureIterator.get();

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
    public void should_return_entities_for_typed_query_async() throws Exception {
        CompleteBean paul = builder().randomId().name("Paul").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Jack").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").buid();

        CompleteBean john = builder().randomId().name("John").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").buid();

        manager.insert(paul);
        manager.insert(john);

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<Object> successSpy1 = new AtomicReference<>();
        final AtomicReference<Object> successSpy2 = new AtomicReference<>();

        FutureCallback<Object> successCallBack1 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy1.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        FutureCallback<Object> successCallBack2 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy2.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        final RegularStatement selectStar = select().from("CompleteBean").where(eq("id", bindMarker("id")));
        final List<CompleteBean> list1 = manager.typedQuery(CompleteBean.class, selectStar, paul.getId()).asyncGet(successCallBack1).get();
        final CompleteBean foundJohn = manager.typedQuery(CompleteBean.class, selectStar, john.getId()).asyncGetFirst(successCallBack2).get();

        assertThat(list1).hasSize(1);

        CompleteBean foundPaul = list1.get(0);

        Factory factory1 = (Factory) foundPaul;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor1 = (ProxyInterceptor<CompleteBean>) factory1.getCallback(0);

        CompleteBean realPaul = (CompleteBean) interceptor1.getTarget();

        assertThat(realPaul.getLabel()).isNull();
        assertThat(realPaul.getWelcomeTweet()).isNull();

        assertThat(realPaul.getName()).isEqualTo(paul.getName());
        assertThat(realPaul.getAge()).isEqualTo(paul.getAge());
        assertThat(realPaul.getFriends()).containsAll(paul.getFriends());
        assertThat(realPaul.getFollowers()).containsAll(paul.getFollowers());
        assertThat(realPaul.getPreferences().get(1)).isEqualTo("FR");
        assertThat(realPaul.getPreferences().get(2)).isEqualTo("Paris");
        assertThat(realPaul.getPreferences().get(3)).isEqualTo("75014");

        Factory factory2 = (Factory) foundJohn;
        @SuppressWarnings("unchecked")
        ProxyInterceptor<CompleteBean> interceptor2 = (ProxyInterceptor<CompleteBean>) factory2.getCallback(0);

        CompleteBean realJohn = (CompleteBean) interceptor2.getTarget();

        assertThat(realJohn.getLabel()).isNull();
        assertThat(realJohn.getWelcomeTweet()).isNull();

        assertThat(realJohn.getName()).isEqualTo(john.getName());
        assertThat(realJohn.getAge()).isEqualTo(john.getAge());
        assertThat(realJohn.getFriends()).containsAll(john.getFriends());
        assertThat(realJohn.getFollowers()).containsAll(john.getFollowers());
        assertThat(realJohn.getPreferences().get(1)).isEqualTo("US");
        assertThat(realJohn.getPreferences().get(2)).isEqualTo("NewYork");

        latch.await();
        Thread.sleep(100);
        assertThat(successSpy1.get()).isNotNull().isInstanceOf(List.class);
        assertThat(successSpy2.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_return_raw_entities_for_raw_typed_query_async() throws Exception {
        Counter counter1 = CounterBuilder.incr(15L);
        CompleteBean paul = builder().randomId().name("Paul").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Jack").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(counter1).buid();

        Counter counter2 = CounterBuilder.incr(17L);
        CompleteBean john = builder().randomId().name("John").age(34L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(counter2).buid();

        manager.insert(paul);
        manager.insert(john);

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<Object> successSpy1 = new AtomicReference<>();
        final AtomicReference<Object> successSpy2 = new AtomicReference<>();

        FutureCallback<Object> successCallBack1 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy1.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        FutureCallback<Object> successCallBack2 = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy2.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        final RegularStatement selectStar = select().from("CompleteBean").where(eq("id", bindMarker("id")));

        final AchillesFuture<CompleteBean> futurePaul = manager.rawTypedQuery(CompleteBean.class, selectStar, paul.getId()).asyncGetFirst(successCallBack1);
        final AchillesFuture<CompleteBean> futureJohn = manager.rawTypedQuery(CompleteBean.class, selectStar, john.getId()).asyncGetFirst(successCallBack2);

        CompleteBean foundPaul = futurePaul.get();

        assertThat(foundPaul.getName()).isEqualTo(paul.getName());
        assertThat(foundPaul.getAge()).isEqualTo(paul.getAge());
        assertThat(foundPaul.getFriends()).containsAll(paul.getFriends());
        assertThat(foundPaul.getFollowers()).containsAll(paul.getFollowers());
        assertThat(foundPaul.getPreferences().get(1)).isEqualTo("FR");
        assertThat(foundPaul.getPreferences().get(2)).isEqualTo("Paris");
        assertThat(foundPaul.getPreferences().get(3)).isEqualTo("75014");
        assertThat(foundPaul.getVersion()).isNull();

        CompleteBean foundJohn = futureJohn.get();
        assertThat(foundJohn.getName()).isEqualTo(john.getName());
        assertThat(foundJohn.getAge()).isEqualTo(john.getAge());
        assertThat(foundJohn.getFriends()).containsAll(john.getFriends());
        assertThat(foundJohn.getFollowers()).containsAll(john.getFollowers());
        assertThat(foundJohn.getPreferences().get(1)).isEqualTo("US");
        assertThat(foundJohn.getPreferences().get(2)).isEqualTo("NewYork");
        assertThat(foundJohn.getVersion()).isNull();

        latch.await();
        Thread.sleep(100);
        assertThat(successSpy1.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
        assertThat(successSpy2.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_query_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1, "name4")
                .async().get()
                .getImmediately();

        assertThat(entities).isEmpty();

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<Object> successSpy = new AtomicReference<>();
        final AtomicReference<Throwable> exceptionSpy = new AtomicReference<>();

        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };

        FutureCallback<Object> exceptionCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                exceptionSpy.getAndSet(t);
                latch.countDown();
            }
        };

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1, "name4")
                .withAsyncListeners(successCallBack)
                .async().get()
                .getImmediately();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 3);
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

        assertThat(successSpy.get()).isNotNull().isInstanceOf(List.class);
        List<CompleteBean> raws = (List<CompleteBean>) successSpy.get();
        assertThat(raws).hasSize(3);
        assertThat(raws.get(0)).isNotInstanceOf(Factory.class);

    }

    @Test
    public void should_query_async_with_custom_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name1")
                .toClusterings(1, "name4")
                .fromExclusiveToInclusiveBounds()
                .orderByDescending()
                .limit(2)
                .async().get()
                .getImmediately();

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 3);
    }

    @Test
    public void should_query_async_with_consistency_level() throws Exception {
        Long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertValues(partitionKey, 1, 5);

        final AchillesFuture<List<ClusteredEntity>> futures = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2")
                .toClusterings(1, "name4")
                .withConsistency(EACH_QUORUM)
                .async().get();

        exception.expect(ExecutionException.class);
        exception.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

        futures.get();
    }

    @Test
    public void should_query_async_with_getFirst() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .async().getOne()
                .getImmediately();

        assertThat(entity).isNull();

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .async().getOne()
                .getImmediately();

        assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 1);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .async().get(3)
                .getImmediately();

        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

        insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
        insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);

        entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .async().getFirstMatching(3,4)
                .getImmediately();

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 41);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 42);

    }

    @Test
    public void should_query_async_with_getLast() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .async().getOne()
                .getImmediately();

        assertThat(entity).isNull();

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .async().getOne()
                .getImmediately();

        assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 5);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .async().get(3)
                .getImmediately();

        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

        insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
        insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);
        insertClusteredEntity(partitionKey, 4, "name43", clusteredValuePrefix + 43);
        insertClusteredEntity(partitionKey, 4, "name44", clusteredValuePrefix + 44);

        entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .async().getLastMatching(3,4)
                .getImmediately();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 44);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 43);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 42);
    }

    @Test
    public void should_iterate_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .async().iterator()
                .getImmediately();

        assertThat(iter.hasNext()).isTrue();
        ClusteredEntity next = iter.next();
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name1");
        assertThat(iter.hasNext()).isTrue();

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name2");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 2);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name3");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 3);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name4");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 4);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name5");
        assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_async_with_custom_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2")
                .toClusterings(1)
                .async()
                .iterator(2).getImmediately();

        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 3);
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_async_over_clusterings_components() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredEntity(partitionKey, 1, "name11", "val11");
        insertClusteredEntity(partitionKey, 1, "name12", "val12");
        insertClusteredEntity(partitionKey, 1, "name13", "val13");
        insertClusteredEntity(partitionKey, 2, "name21", "val21");
        insertClusteredEntity(partitionKey, 2, "name22", "val22");
        insertClusteredEntity(partitionKey, 3, "name31", "val31");
        insertClusteredEntity(partitionKey, 4, "name41", "val41");

        //When
        final Iterator<ClusteredEntity> iterator = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1)
                .fromInclusiveToExclusiveBounds()
                .limit(6)
                .async()
                .iterator(2).getImmediately();

        //Then
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val11");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val12");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val13");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val21");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val22");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("val31");

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_remove_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String clusteredValuePrefix = insertValues(partitionKey, 1, 2);
        insertValues(partitionKey, 2, 3);
        insertValues(partitionKey, 3, 1);

        manager.sliceQuery(ClusteredEntity.class)
                .forDelete()
                .withPartitionComponents(partitionKey)
                .async()
                .deleteMatching(2)
                .getImmediately();

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .get(100);

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);

        assertThat(entities.get(2).getId().getCount()).isEqualTo(3);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 1);
    }

    private String insertValues(long partitionKey, int countValue, int size) {
        String namePrefix = "name";
        String clusteredValuePrefix = "value";

        for (int i = 1; i <= size; i++) {
            insertClusteredEntity(partitionKey, countValue, namePrefix + i, clusteredValuePrefix + i);
        }
        return clusteredValuePrefix;
    }

    private void insertClusteredEntity(Long partitionKey, int count, String name, String clusteredValue) {
        ClusteredEntity.ClusteredKey embeddedId = new ClusteredEntity.ClusteredKey(partitionKey, count, name);
        ClusteredEntity entity = new ClusteredEntity(embeddedId, clusteredValue);
        manager.insert(entity);
    }
}
