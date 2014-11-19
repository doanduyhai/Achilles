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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static info.archinnov.achilles.type.OptionsBuilder.withAsyncListeners;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.persistence.AsyncManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.type.CounterBuilder;
import net.sf.cglib.proxy.Factory;

public class AsyncOperationsIT {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean");

    private AsyncManager manager = resource.getAsyncManager();
    private Session session = manager.getNativeSession();
    final ExecutorService executorService = Executors.newCachedThreadPool();


    @Test
    public void should_persist_many() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        CompleteBean paul = CompleteBeanTestBuilder.builder().randomId().name("Paul").version(CounterBuilder.incr(11L)).buid();
        CompleteBean george = CompleteBeanTestBuilder.builder().randomId().name("George").version(CounterBuilder.incr(12L)).buid();
        CompleteBean michael = CompleteBeanTestBuilder.builder().randomId().name("Michael").version(CounterBuilder.incr(13L)).buid();

        final AtomicReference<Object> spy = new AtomicReference<>();
        FutureCallback<Object> callback = new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                spy.getAndSet(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };

        final AchillesFuture<CompleteBean> futureEntity1 = manager.insert(paul, withAsyncListeners(callback));
        final AchillesFuture<CompleteBean> futureEntity2 = manager.insert(george);
        final AchillesFuture<CompleteBean> futureEntity3 = manager.insert(michael);

        Callable<CompleteBean> callable = new Callable<CompleteBean>() {
            @Override
            public CompleteBean call() throws Exception {
                futureEntity1.get();
                futureEntity2.get();
                final CompleteBean entity3 = futureEntity3.get();
                latch.countDown();
                return entity3;
            }
        };

        final Future<CompleteBean> futureCompletion = executorService.submit(callable);
        futureCompletion.get();
        latch.await();

        Row row1 = session.execute("select name from completebean where id = " + paul.getId()).one();
        Row row2 = session.execute("select name from completebean where id = " + george.getId()).one();
        Row row3 = session.execute("select name from completebean where id = " + michael.getId()).one();

        assertThat(row1.getString("name")).isEqualTo("Paul");
        assertThat(row2.getString("name")).isEqualTo("George");
        assertThat(row3.getString("name")).isEqualTo("Michael");

        final String className = CompleteBean.class.getCanonicalName();
        final Select.Where select1 = QueryBuilder.select("counter_value").from("achilles_counter_table")
                .where(eq("fqcn", className))
                .and(eq("primary_key", paul.getId().toString()))
                .and(eq("property_name", "version"));

        final Select.Where select2 = QueryBuilder.select("counter_value").from("achilles_counter_table")
                .where(eq("fqcn", className))
                .and(eq("primary_key", george.getId().toString()))
                .and(eq("property_name", "version"));

        final Select.Where select3 = QueryBuilder.select("counter_value").from("achilles_counter_table")
                .where(eq("fqcn", className))
                .and(eq("primary_key", michael.getId().toString()))
                .and(eq("property_name", "version"));

        row1 = session.execute(select1).one();
        row2 = session.execute(select2).one();
        row3 = session.execute(select3).one();

        assertThat(row1.getLong("counter_value")).isEqualTo(11L);
        assertThat(row2.getLong("counter_value")).isEqualTo(12L);
        assertThat(row3.getLong("counter_value")).isEqualTo(13L);

        assertThat(spy.get()).isNotNull().isSameAs(paul);
    }

    @Test
    public void should_persist_with_success_and_error_async() throws Exception {
        //Given
        CompleteBean paul = CompleteBeanTestBuilder.builder().randomId().name("Paul").buid();
        CompleteBean george = CompleteBeanTestBuilder.builder().randomId().name("George").buid();

        manager.insert(paul);

        final CountDownLatch latch = new CountDownLatch(1);
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

        FutureCallback<Object> errorCallBack = new FutureCallback<Object>() {
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
        //When
        manager.insert(george, withAsyncListeners(successCallBack));
        manager.insert(paul, withAsyncListeners(errorCallBack).ifNotExists());

        //Then
        latch.await();
        Thread.sleep(100);
        assertThat(successSpy.get()).isNotNull().isSameAs(george);
        assertThat(exceptionSpy.get()).isNotNull().isInstanceOf(Throwable.class);
    }

    @Test
    public void should_not_notify_async_listener_if_cas_listener_provided() throws Exception {
        //Given
        CompleteBean paul = CompleteBeanTestBuilder.builder().randomId().name("Paul").buid();

        manager.insert(paul);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> exceptionSpy = new AtomicReference<>();
        final AtomicReference<LWTResultListener.LWTResult> LWTResultSpy = new AtomicReference<>();

        FutureCallback<Object> errorCallBack = new FutureCallback<Object>() {
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
        //When
        LWTResultListener casListener = new LWTResultListener() {
            @Override
            public void onLWTSuccess() {

            }

            @Override
            public void onLWTError(LWTResult casResult) {
                LWTResultSpy.getAndSet(casResult);
            }
        };
        manager.insert(paul, withAsyncListeners(errorCallBack).ifNotExists().LWTResultListener(casListener));

        //Then
        latch.await();
        Thread.sleep(100);
        assertThat(exceptionSpy.get()).isNull();
        assertThat(LWTResultSpy.get()).isNotNull().isInstanceOf(LWTResultListener.LWTResult.class);
        ;
    }

    @Test
    public void should_find_many() throws Exception {
        CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
        CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId().name("Paul").buid();
        CompleteBean entity3 = CompleteBeanTestBuilder.builder().randomId().name("Michael").buid();

        manager.insert(entity1).getImmediately();
        manager.insert(entity2).getImmediately();
        manager.insert(entity3).getImmediately();

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

        final AchillesFuture<CompleteBean> future1 = manager.find(CompleteBean.class, entity1.getId(), withAsyncListeners(successCallBack));
        final AchillesFuture<CompleteBean> future2 = manager.find(CompleteBean.class, entity2.getId());
        final AchillesFuture<CompleteBean> future3 = manager.find(CompleteBean.class, entity3.getId());

        final CompleteBean found3 = future3.get();
        assertThat(found3).isNotNull();
        assertThat(found3.getName()).isEqualTo("Michael");

        final CompleteBean found1 = future1.get();
        assertThat(found1).isNotNull();
        assertThat(found1.getName()).isEqualTo("Jonathan");

        final CompleteBean found2 = future2.get();
        assertThat(found2).isNotNull();
        assertThat(found2.getName()).isEqualTo("Paul");

        latch.await();
        Thread.sleep(100);
        assertThat(successSpy.get()).isNotNull().isInstanceOf(CompleteBean.class)
                .isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_update_modifications() throws Exception {
        CompleteBean jonathan = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(41L)
                .addFriends("bob", "alice").addPreference(1, "US").addPreference(2, "New York").buid();
        CompleteBean paul = CompleteBeanTestBuilder.builder().randomId().name("Paul").age(42L)
                .addFriends("bob", "alice").addPreference(1, "US").addPreference(2, "San Francisco").buid();
        CompleteBean george = CompleteBeanTestBuilder.builder().randomId().name("George").age(43L)
                .addFriends("bob", "alice").addPreference(1, "US").addPreference(2, "Seattle").buid();

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
        CompleteBean managedJonathan = manager.insert(jonathan).getImmediately();
        managedJonathan.setAge(101L);
        managedJonathan.getFriends().add("eve");
        managedJonathan.getPreferences().put(1, "FR");

        CompleteBean managedPaul = manager.insert(paul).getImmediately();
        managedPaul.setAge(102L);
        managedPaul.getFriends().add("oscar");
        managedPaul.getPreferences().put(1, "CA");

        CompleteBean managedGeorge = manager.insert(george).getImmediately();
        managedGeorge.setAge(103L);
        managedGeorge.getFriends().add("mallory");
        managedGeorge.getPreferences().put(2, "Seattle");

        final AchillesFuture<CompleteBean> futureJonathan = manager.update(managedJonathan, withAsyncListeners(successCallBack));
        final AchillesFuture<CompleteBean> futurePaul = manager.update(managedPaul);
        final AchillesFuture<CompleteBean> futureGeorge = manager.update(managedGeorge);

        futureJonathan.get();
        Row row1 = session.execute("select * from completebean where id=" + jonathan.getId()).one();
        assertThat(row1.getLong("age_in_years")).isEqualTo(101L);
        assertThat(row1.getList("friends", String.class)).containsExactly("bob", "alice", "eve");
        Map<Integer, String> preferences1 = row1.getMap("preferences", Integer.class, String.class);
        assertThat(preferences1.get(1)).isEqualTo("FR");
        assertThat(preferences1.get(2)).isEqualTo("New York");

        futurePaul.get();
        Row row2 = session.execute("select * from completebean where id=" + paul.getId()).one();
        assertThat(row2.getLong("age_in_years")).isEqualTo(102L);
        assertThat(row2.getList("friends", String.class)).containsExactly("bob", "alice", "oscar");
        Map<Integer, String> preferences2 = row2.getMap("preferences", Integer.class, String.class);
        assertThat(preferences2.get(1)).isEqualTo("CA");
        assertThat(preferences2.get(2)).isEqualTo("San Francisco");

        futureGeorge.get();
        Row row3 = session.execute("select * from completebean where id=" + george.getId()).one();
        assertThat(row3.getLong("age_in_years")).isEqualTo(103L);
        assertThat(row3.getList("friends", String.class)).containsExactly("bob", "alice", "mallory");
        Map<Integer, String> preferences3 = row3.getMap("preferences", Integer.class, String.class);
        assertThat(preferences3.get(1)).isEqualTo("US");
        assertThat(preferences3.get(2)).isEqualTo("Seattle");

        latch.await();
        Thread.sleep(100);
        assertThat(successSpy.get()).isNotNull().isSameAs(managedJonathan);
    }


    @Test
    public void should_remove_many() throws Exception {
        CompleteBean paul = CompleteBeanTestBuilder.builder().randomId().name("Paul").buid();
        CompleteBean jack = CompleteBeanTestBuilder.builder().randomId().name("Jack").buid();
        CompleteBean john = CompleteBeanTestBuilder.builder().randomId().name("John").buid();

        paul = manager.insert(paul).getImmediately();
        jack = manager.insert(jack).getImmediately();
        john = manager.insert(john).getImmediately();

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

        final AchillesFuture<CompleteBean> futurePaul = manager.delete(paul, withAsyncListeners(successCallBack));
        final AchillesFuture<CompleteBean> futureJack = manager.delete(jack);
        final AchillesFuture<CompleteBean> futureJohn = manager.delete(john);

        futureJohn.get();
        List<Row> rowsJohn = session.execute("select * from completebean where id=" + john.getId()).all();
        assertThat(rowsJohn).isEmpty();

        futurePaul.get();
        List<Row> rowsPaul = session.execute("select * from completebean where id=" + paul.getId()).all();
        assertThat(rowsPaul).isEmpty();

        futureJack.get();
        List<Row> rowsJack = session.execute("select * from completebean where id=" + jack.getId()).all();
        assertThat(rowsJack).isEmpty();

        latch.await();
        Thread.sleep(100);
        assertThat(successSpy.get()).isNotNull().isInstanceOf(CompleteBean.class)
                .isNotExactlyInstanceOf(Factory.class);
    }

    @Test
    public void should_refresh_many() throws Exception {

        CompleteBean paul = CompleteBeanTestBuilder.builder().randomId().name("Paul").addFriends("bob", "alice").age(35L).buid();
        CompleteBean john = CompleteBeanTestBuilder.builder().randomId().name("John").addFriends("bob", "alice").age(35L).buid();
        CompleteBean jack = CompleteBeanTestBuilder.builder().randomId().name("Jack").addFriends("bob", "alice").age(35L).buid();

        paul = manager.insert(paul).getImmediately();
        john = manager.insert(john).getImmediately();
        jack = manager.insert(jack).getImmediately();

        session.execute("UPDATE completebean SET name='Paul_modified' WHERE id=" + paul.getId());
        session.execute("UPDATE completebean SET friends=friends + ['eve'] WHERE id=" + paul.getId());

        session.execute("UPDATE completebean SET name='John_modified' WHERE id=" + john.getId());
        session.execute("UPDATE completebean SET friends=friends + ['oscar'] WHERE id=" + john.getId());

        session.execute("UPDATE completebean SET name='Jack_modified' WHERE id=" + jack.getId());
        session.execute("UPDATE completebean SET friends=friends + ['mallory'] WHERE id=" + jack.getId());

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

        final AchillesFuture<CompleteBean> futurePaul = manager.refresh(paul, withAsyncListeners(successCallBack));
        final AchillesFuture<CompleteBean> futureJohn = manager.refresh(john);
        final AchillesFuture<CompleteBean> futureJack = manager.refresh(jack);

        futurePaul.get();
        assertThat(paul.getName()).isEqualTo("Paul_modified");
        assertThat(paul.getFriends()).hasSize(3);
        assertThat(paul.getFriends().get(2)).isEqualTo("eve");

        futureJohn.get();
        assertThat(john.getName()).isEqualTo("John_modified");
        assertThat(john.getFriends()).hasSize(3);
        assertThat(john.getFriends().get(2)).isEqualTo("oscar");

        futureJack.get();
        assertThat(jack.getName()).isEqualTo("Jack_modified");
        assertThat(jack.getFriends()).hasSize(3);
        assertThat(jack.getFriends().get(2)).isEqualTo("mallory");

        latch.await();
        Thread.sleep(100);
        assertThat(successSpy.get()).isNotNull().isInstanceOf(CompleteBean.class)
                .isNotExactlyInstanceOf(Factory.class);
    }

    @Test
    public void should_notified_async_listener_for_staled_object_on_refresh() throws Exception {
        //Given
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Object> exceptionSpy = new AtomicReference<>();

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
        final CompleteBean proxy = manager.getProxy(CompleteBean.class, 10L).getImmediately();

        //When
        manager.refresh(proxy, withAsyncListeners(exceptionCallBack));

        latch.await();
        Thread.sleep(100);

        //Then
        assertThat(exceptionSpy.get()).isNotNull().isInstanceOf(AchillesStaleObjectStateException.class);
    }
}
