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

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.TWO;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.Batch;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;

public class BatchModeIT {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
            "Tweet", "User");
    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    private User user;

    private Long userId = RandomUtils.nextLong(0,Long.MAX_VALUE);

    @Before
    public void setUp() {
        user = UserTestBuilder.user().id(userId).firstname("fn").lastname("ln").buid();
    }

    @Test
    public void should_batch_counters() throws Exception {
        // Start batch
        Batch batch = manager.createBatch();
        batch.startBatch();

        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

        entity = batch.insert(entity);

        entity.setLabel("label");

        Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("welcomeTweet").buid();
        entity.setWelcomeTweet(welcomeTweet);

        entity.getVersion().incr(10L);
        batch.update(entity);

        final RegularStatement selectLabel = select("label").from("CompleteBean").where(eq("id", entity.getId()));
        Map<String, Object> result = manager.nativeQuery(selectLabel).first();
        assertThat(result).isNull();

        RegularStatement selectCounter = select("counter_value").from("achilles_counter_table")
                .where(eq("fqcn", CompleteBean.class.getCanonicalName()))
                .and(eq("primary_key", entity.getId().toString()))
                .and(eq("property_name", "version"));

        result = manager.nativeQuery(selectCounter).first();
        assertThat(result).isNull();

        // Flush
        batch.endBatch();

        Row row = manager.getNativeSession().execute(selectLabel).one();
        assertThat(row.getString("label")).isEqualTo("label");

        result = manager.nativeQuery(selectCounter).first();
        assertThat(result.get("counter_value")).isEqualTo(10L);
        assertThatBatchContextHasBeenReset(batch);
    }

    @Test
    public void should_batch_several_entities() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("tweet2").buid();

        // Start batch
        Batch batch = manager.createBatch();
        batch.startBatch();

        batch.insert(bean);
        batch.insert(tweet1);
        batch.insert(tweet2);
        batch.insert(user);

        CompleteBean foundBean = manager.find(CompleteBean.class, bean.getId());
        Tweet foundTweet1 = manager.find(Tweet.class, tweet1.getId());
        Tweet foundTweet2 = manager.find(Tweet.class, tweet2.getId());
        User foundUser = manager.find(User.class, user.getId());

        assertThat(foundBean).isNull();
        assertThat(foundTweet1).isNull();
        assertThat(foundTweet2).isNull();
        assertThat(foundUser).isNull();

        // Flush
        batch.endBatch();

        final ResultSet resultSet = manager.getNativeSession().execute("SELECT id,favoriteTweets,followers,friends,age_in_years,name,welcomeTweet,label,preferences FROM CompleteBean WHERE id=:id", bean.getId());
        assertThat(resultSet.all()).hasSize(1);

        foundBean = manager.find(CompleteBean.class, bean.getId());
        foundTweet1 = manager.find(Tweet.class, tweet1.getId());
        foundTweet2 = manager.find(Tweet.class, tweet2.getId());
        foundUser = manager.find(User.class, user.getId());

        assertThat(foundBean.getName()).isEqualTo("name");
        assertThat(foundTweet1.getContent()).isEqualTo("tweet1");
        assertThat(foundTweet2.getContent()).isEqualTo("tweet2");
        assertThat(foundUser.getFirstname()).isEqualTo("fn");
        assertThat(foundUser.getLastname()).isEqualTo("ln");
        assertThatBatchContextHasBeenReset(batch);
    }

    @Test
    public void should_reinit_batch_context_after_exception() throws Exception {
        User user = UserTestBuilder.user().id(123456494L).firstname("firstname").lastname("lastname").buid();
        Tweet tweet = TweetTestBuilder.tweet().randomId().content("simple_tweet").creator(user).buid();

        user = manager.insert(user);

        // Start batch
        Batch batch = manager.createBatch();
        batch.startBatch();

        boolean exceptionCaught = false;

        try {
            batch.insert(tweet, OptionsBuilder.withConsistency(ConsistencyLevel.EACH_QUORUM));
        } catch (AchillesException e) {
            exceptionCaught = true;
            batch.cleanBatch();
            assertThatBatchContextHasBeenReset(batch);

            assertThat(manager.find(Tweet.class, tweet.getId())).isNull();
        }

        // batch should reinit batch context
        batch.insertOrUpdate(user);
        batch.endBatch();

        User foundUser = manager.find(User.class, user.getId());
        assertThat(foundUser.getFirstname()).isEqualTo("firstname");
        assertThat(foundUser.getLastname()).isEqualTo("lastname");

        batch.insert(tweet);
        batch.endBatch();

        Tweet foundTweet = manager.find(Tweet.class, tweet.getId());
        assertThat(foundTweet.getContent()).isEqualTo("simple_tweet");
        assertThat(foundTweet.getCreator().getId()).isEqualTo(foundUser.getId());
        assertThat(foundTweet.getCreator().getFirstname()).isEqualTo("firstname");
        assertThat(foundTweet.getCreator().getLastname()).isEqualTo("lastname");
        assertThatBatchContextHasBeenReset(batch);
    }

    @Test
    public void should_batch_with_custom_consistency_level() throws Exception {
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();
        Tweet tweet3 = TweetTestBuilder.tweet().randomId().content("simple_tweet3").buid();

        manager.insert(tweet1);

        // Start batch
        Batch batch = manager.createBatch();
        batch.startBatch();

        batch.startBatch(QUORUM);

        Tweet foundTweet1 = manager.find(Tweet.class, tweet1.getId());

        assertThat(foundTweet1.getContent()).isEqualTo(tweet1.getContent());

        batch.insert(tweet2);
        batch.insert(tweet3);

        logAsserter.prepareLogLevel();

        batch.endBatch();

        logAsserter.assertConsistencyLevels(QUORUM);
        assertThatBatchContextHasBeenReset(batch);
    }

    @Test
    public void should_reinit_batch_context_and_consistency_after_exception() throws Exception {
        boolean exceptionCaught = false;
        Tweet tweet1 = TweetTestBuilder.tweet().randomId().content("simple_tweet1").buid();
        Tweet tweet2 = TweetTestBuilder.tweet().randomId().content("simple_tweet2").buid();

        manager.insert(tweet1);

        // Start batch
        Batch batch = manager.createBatch();
        batch.startBatch();

        batch.startBatch(TWO);
        batch.insertOrUpdate(tweet2);

        try {
            batch.endBatch();
        } catch (Exception e) {
            assertThatBatchContextHasBeenReset(batch);
            exceptionCaught = true;
        }

        assertThat(exceptionCaught).isTrue();

        Thread.sleep(1000);
        logAsserter.prepareLogLevel();
        batch.insert(tweet2);
        batch.endBatch();
        logAsserter.assertConsistencyLevels(ONE);
    }

    @Test
    public void should_order_batch_operations_on_the_same_column_with_insert_and_update() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        final Batch batch = manager.createOrderedBatch();

        //When
        batch.startBatch();

        entity = batch.insert(entity);
        entity.setLabel("label");
        batch.update(entity);

        batch.endBatch();

        //Then
        Statement statement = new SimpleStatement("SELECT label from CompleteBean where id=" + entity.getId());
        Row row = manager.getNativeSession().execute(statement).one();
        assertThat(row.getString("label")).isEqualTo("label");
    }


    @Test
    public void should_order_batch_operations_on_the_same_column() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name1000").buid();
        final Batch batch = manager.createOrderedBatch();

        //When
        batch.startBatch();

        entity = batch.insert(entity);
        entity.setName("name");
        batch.update(entity);

        batch.endBatch();

        //Then
        Statement statement = new SimpleStatement("SELECT name from CompleteBean where id=" + entity.getId());
        Row row = manager.getNativeSession().execute(statement).one();
        assertThat(row.getString("name")).isEqualTo("name");
    }

    @Test
    public void should_mix_batch_with_native_statement() throws Exception {
        //Given
        CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId().name("name1000").buid();
        CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId().name("name1000").buid();

        manager.insert(entity2);

        final Batch batch = manager.createBatch();

        batch.startBatch();

        batch.insert(entity1);

        final Update.Where statement = update(CompleteBean.TABLE_NAME).with(set("name", bindMarker("name")))
                .where(eq("id", bindMarker("id")));

        //When
        batch.batchNativeStatement(statement,"DuyHai",entity2.getId());
        batch.endBatch();

        //Then
        Statement select = new SimpleStatement("SELECT name from CompleteBean where id=" + entity2.getId());
        Row row = manager.getNativeSession().execute(select).one();
        assertThat(row.getString("name")).isEqualTo("DuyHai");
    }

    @Test
    public void should_batch_native_statement_with_CAS_result_listener() throws Exception {
        //Given
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name1000").buid();
        manager.insert(entity);
        final Batch batch = manager.createBatch();
        final Insert statement = insertInto("CompleteBean").value("id", bindMarker("id")).value("name", bindMarker("name")).ifNotExists();

        final AtomicBoolean error = new AtomicBoolean(false);
        final AtomicReference<CASResultListener.CASResult> result = new AtomicReference<>(null);

        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {

            }

            @Override
            public void onCASError(CASResult casResult) {
                error.getAndSet(true);
                result.getAndSet(casResult);
            }
        };

        //When
        batch.batchNativeStatementWithCASListener(statement,listener, entity.getId(),"name");
        batch.endBatch();

        //Then
        assertThat(error.get()).isTrue();
        assertThat(result.get()).isNotNull();

        final TypedMap currentValues = result.get().currentValues();

        assertThat(currentValues.getTyped("id")).isEqualTo(entity.getId());
        assertThat(currentValues.getTyped("name")).isEqualTo("name1000");

    }

    @Test
    public void should_batch_regular_statement() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String name = "DuyHai";
        final Insert statement = insertInto("CompleteBean").value("id", id).value("name", name).ifNotExists();

        final Batch batch = manager.createBatch();

        batch.startBatch();

        batch.batchNativeStatement(statement);

        batch.endBatch();

        //When
        final CompleteBean found = manager.find(CompleteBean.class, id);

        //Then
        assertThat(found.getName()).isEqualTo(name);
    }

    @Test
    public void should_batch_bound_statement() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        String name = "DuyHai";
        final Insert insert = insertInto("CompleteBean").value("id", bindMarker("id")).value("name", bindMarker("name")).ifNotExists();
        final PreparedStatement ps = manager.getNativeSession().prepare(insert);
        final BoundStatement bs = ps.bind(id, name);

        final Batch batch = manager.createBatch();

        batch.startBatch();

        batch.batchNativeStatement(bs);

        batch.endBatch();

        //When
        final CompleteBean found = manager.find(CompleteBean.class, id);

        //Then
        assertThat(found.getName()).isEqualTo(name);
    }

    private void assertThatBatchContextHasBeenReset(Batch batchEm) {
        BatchingFlushContext flushContext = Whitebox.getInternalState(batchEm, BatchingFlushContext.class);
        ConsistencyLevel consistencyLevel = Whitebox.getInternalState(flushContext, "consistencyLevel");
        List<AbstractStatementWrapper> statementWrappers = Whitebox.getInternalState(flushContext, "statementWrappers");

        assertThat(consistencyLevel).isEqualTo(ConsistencyLevel.ONE);
        assertThat(statementWrappers).isEmpty();
    }
}
