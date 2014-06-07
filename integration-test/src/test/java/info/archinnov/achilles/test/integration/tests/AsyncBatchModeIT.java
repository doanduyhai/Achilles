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
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import info.archinnov.achilles.persistence.*;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;

public class AsyncBatchModeIT {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
            "Tweet", "User");

    private AsyncManager manager = resource.getAsyncManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    private User user;

    private Long userId = RandomUtils.nextLong(0,Long.MAX_VALUE);

    @Before
    public void setUp() {
        user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();
    }

    @Test
    public void should_batch_counters_async() throws Exception {
        // Start batch
        AsyncBatch batch = manager.createBatch();
        batch.startBatch();

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

        entity = batch.insert(entity);

        entity.setLabel("label");

        Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("welcomeTweet").buid();
        entity.setWelcomeTweet(welcomeTweet);

        entity.getVersion().incr(10L);
        batch.update(entity);

        RegularStatement selectLabel = select("label").from("CompleteBean").where(eq("id",entity.getId()));
        Map<String, Object> result = manager.nativeQuery(selectLabel).first();
        assertThat(result).isNull();

        RegularStatement selectCounter = select("counter_value")
                .from("achilles_counter_table")
                .where(eq("fqcn",CompleteBean.class.getCanonicalName()))
                .and(eq("primary_key",entity.getId().toString()))
                .and(eq("property_name","version"));

        result = manager.nativeQuery(selectCounter).first();

        assertThat(result).isNull();

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

        // Flush
        batch.endBatch(successCallBack, errorCallBack);

        latch.await();

        Statement statement = new SimpleStatement("SELECT label from CompleteBean where id=" + entity.getId());
        Row row = manager.getNativeSession().execute(statement).one();
        assertThat(row.getString("label")).isEqualTo("label");

        result = manager.nativeQuery(selectCounter).first();
        assertThat(result.get("counter_value")).isEqualTo(10L);
        assertThatBatchContextHasBeenReset(batch);

        assertThat(successSpy.get()).isNotNull().isSameAs(Empty.INSTANCE);
        assertThat(exceptionSpy.get()).isNull();
    }

    @Test
    public void should_batch_several_entities_async() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();

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

        // Start batch
        AsyncBatch batch = manager.createBatch();
        batch.startBatch();

        batch.insert(bean);
        batch.insert(tweet1);
        batch.insert(tweet2);
        batch.insert(user);

        CompleteBean foundBean = manager.find(CompleteBean.class, bean.getId()).getImmediately();
        Tweet foundTweet1 = manager.find(Tweet.class, tweet1.getId()).getImmediately();
        Tweet foundTweet2 = manager.find(Tweet.class, tweet2.getId()).getImmediately();
        User foundUser = manager.find(User.class, user.getId()).getImmediately();

        assertThat(foundBean).isNull();
        assertThat(foundTweet1).isNull();
        assertThat(foundTweet2).isNull();
        assertThat(foundUser).isNull();

        // Flush
        batch.endBatch(successCallBack, errorCallBack);

        latch.await();

        final ResultSet resultSet = manager.getNativeSession().execute("SELECT id,favoriteTweets,followers,friends,age_in_years,name,welcomeTweet,label,preferences FROM CompleteBean WHERE id=:id", bean.getId());
        assertThat(resultSet.all()).hasSize(1);

        foundBean = manager.find(CompleteBean.class, bean.getId()).getImmediately();
        foundTweet1 = manager.find(Tweet.class, tweet1.getId()).getImmediately();
        foundTweet2 = manager.find(Tweet.class, tweet2.getId()).getImmediately();
        foundUser = manager.find(User.class, user.getId()).getImmediately();

        assertThat(foundBean.getName()).isEqualTo("name");
        assertThat(foundTweet1.getContent()).isEqualTo("tweet1");
        assertThat(foundTweet2.getContent()).isEqualTo("tweet2");
        assertThat(foundUser.getFirstname()).isEqualTo("fn");
        assertThat(foundUser.getLastname()).isEqualTo("ln");
        assertThatBatchContextHasBeenReset(batch);

        assertThat(successSpy.get()).isNotNull().isSameAs(Empty.INSTANCE);
        assertThat(exceptionSpy.get()).isNull();
    }

    @Test
    public void should_batch_with_custom_consistency_level_async() throws Exception {
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();
        Tweet tweet3 = TweetTestBuilder.tweet().randomId().content("simple_tweet3").buid();

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

        manager.insert(tweet1);

        // Start batch
        AsyncBatch batch = manager.createBatch();

        batch.startBatch(QUORUM);

        logAsserter.prepareLogLevel();

        Tweet foundTweet1 = manager.find(Tweet.class, tweet1.getId()).getImmediately();

        assertThat(foundTweet1.getContent()).isEqualTo(tweet1.getContent());

        batch.insert(tweet2);
        batch.insert(tweet3);

        batch.endBatch(successCallBack);

        latch.await();

        logAsserter.assertConsistencyLevels(QUORUM);
        assertThatBatchContextHasBeenReset(batch);

        assertThat(successSpy.get()).isSameAs(Empty.INSTANCE);
    }

    @Test
    public void should_reinit_batch_context_and_consistency_after_exception_async() throws Exception {
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();

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

        manager.insert(tweet1);

        // Start batch
        AsyncBatch batch = manager.createBatch();

        batch.startBatch(EACH_QUORUM);
        batch.insert(tweet2);

        batch.endBatch(successCallBack);

        latch.await();

        assertThatBatchContextHasBeenReset(batch);

        logAsserter.prepareLogLevel();
        batch.startBatch();
        batch.insert(tweet2);
        batch.endBatch();
        logAsserter.assertConsistencyLevels(ONE);

        assertThat(successSpy.get()).isEqualTo(Empty.INSTANCE);
    }

    @Test
    public void should_order_batch_operations_on_the_same_column_with_insert_and_update_async() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        final AsyncBatch batch = manager.createOrderedBatch();

        //When
        batch.startBatch();

        entity = batch.insert(entity);
        entity.setLabel("label");
        batch.update(entity);

        batch.endBatch().getImmediately();

        //Then
        Statement statement = new SimpleStatement("SELECT label from CompleteBean where id=" + entity.getId());
        Row row = manager.getNativeSession().execute(statement).one();
        assertThat(row.getString("label")).isEqualTo("label");
    }


    @Test
    public void should_order_batch_operations_on_the_same_column_async() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name1000").buid();
        final AsyncBatch batch = manager.createOrderedBatch();

        //When
        batch.startBatch();

        entity = batch.insert(entity);
        entity.setName("name");
        batch.update(entity);

        batch.endBatch().getImmediately();

        //Then
        Statement statement = new SimpleStatement("SELECT name from CompleteBean where id=" + entity.getId());
        Row row = manager.getNativeSession().execute(statement).one();
        assertThat(row.getString("name")).isEqualTo("name");
    }

    private void assertThatBatchContextHasBeenReset(AsyncBatch batch) {
        BatchingFlushContext flushContext = Whitebox.getInternalState(batch, BatchingFlushContext.class);
        ConsistencyLevel consistencyLevel = Whitebox.getInternalState(flushContext, "consistencyLevel");
        List<AbstractStatementWrapper> statementWrappers = Whitebox.getInternalState(flushContext, "statementWrappers");

        assertThat(consistencyLevel).isEqualTo(ConsistencyLevel.ONE);
        assertThat(statementWrappers).isEmpty();
    }
}
