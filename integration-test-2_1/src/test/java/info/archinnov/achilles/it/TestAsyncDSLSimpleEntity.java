/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.it;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;

public class TestAsyncDSLSimpleEntity {

    private static final String ASYNC_LOGGER_STRING = "info.archinnov.achilles.AsyncLogger";
    private static final Logger LOGGER = LoggerFactory.getLogger(ASYNC_LOGGER_STRING);
    private static final String CALLED = "Called";

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(SimpleEntity.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();

    @Test
    public void should_dsl_select_slice_async() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date1 = dateFormat.parse("2015-10-01 00:00:00 GMT");
        final Date date9 = dateFormat.parse("2015-10-09 00:00:00 GMT");
        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");
        values.put("date6", "'2015-10-06 00:00:00+0000'");
        values.put("date7", "'2015-10-07 00:00:00+0000'");
        values.put("date8", "'2015-10-08 00:00:00+0000'");
        values.put("date9", "'2015-10-09 00:00:00+0000'");
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_many_rows.cql", values);

        final CountDownLatch latch = new CountDownLatch(1);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(ASYNC_LOGGER_STRING, "%msg - [%thread]%n");

        //When
        final CompletableFuture<List<SimpleEntity>> future = manager
                .dsl()
                .select()
                .consistencyList()
                .simpleSet()
                .simpleMap()
                .value()
                .simpleMap()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Gte_And_Lt(date1, date9)
                .withResultSetAsyncListener(rs -> {
                    LOGGER.info(CALLED);
                    latch.countDown();
                    return rs;
                })
                .withTracing()
                .getListAsync();

        //Then
        latch.await();
        assertThat(future.get()).hasSize(8);
        logAsserter.assertContains("Called - [achilles-default-executor");
    }

    @Test
    public void should_dsl_delete_async() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final CountDownLatch latch = new CountDownLatch(1);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(ASYNC_LOGGER_STRING, "%msg - [%thread]%n");

        //When
        manager
                .dsl()
                .delete()
                .consistencyList()
                .simpleMap()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .withResultSetAsyncListener(rs -> {
                    LOGGER.info(CALLED);
                    latch.countDown();
                    return rs;
                })
                .withTracing()
                .executeAsync();

        //Then
        latch.await();
        logAsserter.assertContains("Called - ");
    }

    @Test
    public void should_dsl_update_value_async() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final AtomicBoolean success = new AtomicBoolean(false);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final CountDownLatch latch = new CountDownLatch(1);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(ASYNC_LOGGER_STRING, "%msg - [%thread]%n");

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new value")
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .if_Value().Eq("0 AM")
                .withLwtResultListener(new LWTResultListener() {
                    @Override
                    public void onSuccess() {
                        success.getAndSet(true);
                    }

                    @Override
                    public void onError(LWTResult lwtResult) {
                    }
                })
                .withResultSetAsyncListener(rs -> {
                    LOGGER.info(CALLED);
                    latch.countDown();
                    return rs;
                })
                .executeAsync();

        //Then
        latch.await();
        logAsserter.assertContains("Called - [achilles-default-executor");
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
