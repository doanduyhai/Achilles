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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import info.archinnov.achilles.persistence.AsyncManager;
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

    private AsyncManager asyncManager = resource.getAsyncManager();

    @Test
    public void should_return_rows_for_native_query_async() throws Exception {
        CompleteBean entity1 = builder().randomId().name("DuyHai").age(35L)
                .addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
                .addPreference(2, "Paris").addPreference(3, "75014").version(CounterBuilder.incr(15L)).buid();

        CompleteBean entity2 = builder().randomId().name("John DOO").age(35L)
                .addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
                .addPreference(2, "NewYork").version(CounterBuilder.incr(17L)).buid();

        asyncManager.insert(entity1).getImmediately();
        asyncManager.insert(entity2).getImmediately();


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


        final AchillesFuture<List<TypedMap>> future1 = asyncManager.nativeQuery(statement, entity1.getId()).get(successCallBack);
        final AchillesFuture<List<TypedMap>> future2 = asyncManager.nativeQuery(statement, entity2.getId()).get();

        latch.await();

        final List<TypedMap> typedMaps1 = future1.get();
        assertThat(typedMaps1).hasSize(1);
        TypedMap typedMap1 = typedMaps1.get(0);

        while (!future2.isDone()) {
            Thread.sleep(2);
        }

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
        asyncManager.nativeQuery(insert, id).execute(successCallBack);
        asyncManager.nativeQuery(delete).execute(exceptionCallBack);

        latch.await();

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

        asyncManager.insert(entity1).getImmediately();
        asyncManager.insert(entity2).getImmediately();
        asyncManager.insert(entity3).getImmediately();
        asyncManager.insert(entity4).getImmediately();
        asyncManager.insert(entity5).getImmediately();

        List<String> possibleNames = Arrays.asList("DuyHai", "Paul", "George", "John", "Helen");

        RegularStatement statement = select().all().from("CompleteBean").limit(6);
        statement.setFetchSize(2);

        final AchillesFuture<Iterator<TypedMap>> futureIterator = asyncManager.nativeQuery(statement).iterator();

        while (!futureIterator.isDone()) {
            Thread.sleep(2);
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

        asyncManager.insert(paul).getImmediately();
        asyncManager.insert(john).getImmediately();

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
        final List<CompleteBean> list1 = asyncManager.typedQuery(CompleteBean.class, selectStar, paul.getId()).get(successCallBack1).get();
        final CompleteBean foundJohn = asyncManager.typedQuery(CompleteBean.class, selectStar, john.getId()).getFirst(successCallBack2).get();

        latch.await();

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

        asyncManager.insert(paul).getImmediately();
        asyncManager.insert(john).getImmediately();

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

        final AchillesFuture<CompleteBean> futurePaul = asyncManager.rawTypedQuery(CompleteBean.class, selectStar, paul.getId()).getFirst(successCallBack1);
        final AchillesFuture<CompleteBean> futureJohn = asyncManager.rawTypedQuery(CompleteBean.class, selectStar, john.getId()).getFirst(successCallBack2);

        latch.await();

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

        assertThat(successSpy1.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
        assertThat(successSpy2.get()).isNotNull().isInstanceOf(CompleteBean.class).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_query_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final CountDownLatch latch1 = new CountDownLatch(1);
        AchillesFuture<List<ClusteredEntity>> futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1, "name4")
                .withAsyncListeners(countDownLatch(latch1))
                .get();

        latch1.await();

        List<ClusteredEntity> entities = futureEntities.get();
        assertThat(entities).isEmpty();

        final CountDownLatch latch2 = new CountDownLatch(1);
        final AtomicReference<Object> successSpy = new AtomicReference<>();

        FutureCallback<Object> successCallBack = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                successSpy.getAndSet(result);
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch2.countDown();
            }
        };

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2").toClusterings(1, "name4")
                .withAsyncListeners(successCallBack)
                .get();

        latch2.await();

        entities = futureEntities.get();

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
        final CountDownLatch latch1 = new CountDownLatch(1);

        AchillesFuture<List<ClusteredEntity>> futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name1")
                .toClusterings(1, "name4")
                .fromExclusiveToInclusiveBounds()
                .orderByDescending()
                .limit(2)
                .withAsyncListeners(countDownLatch(latch1))
                .get();

        latch1.await();

        final List<ClusteredEntity> entities = futureEntities.get();
        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 3);
    }

    @Test
    public void should_query_async_with_consistency_level() throws Exception {
        Long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertValues(partitionKey, 1, 5);

        final AchillesFuture<List<ClusteredEntity>> futures = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2")
                .toClusterings(1, "name4")
                .withConsistency(EACH_QUORUM)
                .get();

        exception.expect(ExecutionException.class);
        exception.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

        futures.get();
    }

    @Test
    public void should_query_async_with_getFirst() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final CountDownLatch latch1 = new CountDownLatch(1);

        AchillesFuture<ClusteredEntity> futureEntity = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch1))
                .getOne();

        latch1.await();

        ClusteredEntity entity = futureEntity.get();
        assertThat(entity).isNull();

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        final CountDownLatch latch2 = new CountDownLatch(1);

        futureEntity = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch2))
                .getOne();

        latch2.await();

        entity = futureEntity.get();
        assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 1);

        final CountDownLatch latch3 = new CountDownLatch(1);

        AchillesFuture<List<ClusteredEntity>> futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch3))
                .get(3);

        latch3.await();

        List<ClusteredEntity> entities = futureEntities.get();
        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

        insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
        insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);

        final CountDownLatch latch4 = new CountDownLatch(1);

        futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch4))
                .getFirstMatching(3, 4);

        latch4.await();

        entities = futureEntities.get();
        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 41);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 42);

    }

    @Test
    public void should_query_async_with_getLast() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        final CountDownLatch latch1 = new CountDownLatch(1);

        AchillesFuture<ClusteredEntity> futureEntity = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch1))
                .orderByDescending()
                .getOne();

        latch1.await();

        ClusteredEntity entity = futureEntity.get();
        assertThat(entity).isNull();

        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        final CountDownLatch latch2 = new CountDownLatch(1);

        futureEntity = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .withAsyncListeners(countDownLatch(latch2))
                .getOne();

        latch2.await();

        entity = futureEntity.get();

        assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 5);

        final CountDownLatch latch3 = new CountDownLatch(1);

        AchillesFuture<List<ClusteredEntity>> futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .withAsyncListeners(countDownLatch(latch3))
                .get(3);

        latch3.await();

        List<ClusteredEntity> entities = futureEntities.get();
        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 5);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 4);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

        insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
        insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);
        insertClusteredEntity(partitionKey, 4, "name43", clusteredValuePrefix + 43);
        insertClusteredEntity(partitionKey, 4, "name44", clusteredValuePrefix + 44);

        final CountDownLatch latch4 = new CountDownLatch(1);

        futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch4))
                .getLastMatching(3, 4);

        latch4.await();

        entities = futureEntities.get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 44);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 43);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 42);
    }

    @Test
    public void should_iterate_async_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

        final CountDownLatch latch1 = new CountDownLatch(1);

        AchillesFuture<Iterator<ClusteredEntity>> futureIter = asyncManager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch1))
                .iterator();

        latch1.await();
        final Iterator<ClusteredEntity> iter = futureIter.get();

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

        final CountDownLatch latch1 = new CountDownLatch(1);

        AchillesFuture<Iterator<ClusteredEntity>> futureIter = asyncManager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2")
                .toClusterings(1)
                .withAsyncListeners(countDownLatch(latch1))
                .iterator(2);

        latch1.await();

        final Iterator<ClusteredEntity> iter = futureIter.get();

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

        final CountDownLatch latch1 = new CountDownLatch(1);

        //When
        AchillesFuture<Iterator<ClusteredEntity>> futureIter = asyncManager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1)
                .fromInclusiveToExclusiveBounds()
                .limit(6)
                .withAsyncListeners(countDownLatch(latch1))
                .iterator(2);

        latch1.await();

        final Iterator<ClusteredEntity> iterator = futureIter.get();

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

        final CountDownLatch latch1 = new CountDownLatch(1);

        asyncManager.sliceQuery(ClusteredEntity.class)
                .forDelete()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch1))
                .deleteMatching(2);

        latch1.await();

        final CountDownLatch latch2 = new CountDownLatch(1);

        AchillesFuture<List<ClusteredEntity>> futureEntities = asyncManager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .withAsyncListeners(countDownLatch(latch2))
                .get(100);

        latch2.await();

        final List<ClusteredEntity> entities = futureEntities.get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);

        assertThat(entities.get(2).getId().getCount()).isEqualTo(3);
        assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 1);
    }

    @Test
    public void should_allow_native_and_typed_query_with_bound_statement() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final Session session = asyncManager.getNativeSession();
        final PreparedStatement insertPs = session.prepare(insertInto(CompleteBean.TABLE_NAME)
                .value("id", bindMarker("id"))
                .value("label", bindMarker("label"))
                .value("age_in_years", bindMarker("age")));
        final BoundStatement insertBs = insertPs.bind(id, "label", 32L);

        final CountDownLatch latch1 = new CountDownLatch(1);
        asyncManager.nativeQuery(insertBs).execute(countDownLatch(latch1));

        latch1.await();

        final PreparedStatement selectPs = session.prepare(select().from(CompleteBean.TABLE_NAME).where(eq("id", bindMarker("id"))));
        final BoundStatement selectBs = selectPs.bind(id);

        //When
        final CountDownLatch latch2 = new CountDownLatch(2);
        final AchillesFuture<CompleteBean> foundWithProxy = asyncManager.typedQuery(CompleteBean.class, selectBs).getFirst(countDownLatch(latch2));
        final AchillesFuture<CompleteBean> foundRaw = asyncManager.rawTypedQuery(CompleteBean.class, selectBs).getFirst(countDownLatch(latch2));

        latch2.await();

        final CompleteBean entityWithProxy = foundWithProxy.get();
        final CompleteBean entityRaw = foundRaw.get();

        //Then
        assertThat(entityWithProxy).isNotNull();
        assertThat(entityWithProxy.getLabel()).isEqualTo("label");
        assertThat(entityWithProxy.getAge()).isEqualTo(32L);

        assertThat(entityRaw).isNotNull();
        assertThat(entityRaw.getLabel()).isEqualTo("label");
        assertThat(entityRaw.getAge()).isEqualTo(32L);
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
        ClusteredEntity.CompoundPK compoundPK = new ClusteredEntity.CompoundPK(partitionKey, count, name);
        ClusteredEntity entity = new ClusteredEntity(compoundPK, clusteredValue);
        asyncManager.insert(entity).getImmediately();
    }

    private FutureCallback<Object> countDownLatch(final CountDownLatch latch) {
        return new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        };
    }
}
