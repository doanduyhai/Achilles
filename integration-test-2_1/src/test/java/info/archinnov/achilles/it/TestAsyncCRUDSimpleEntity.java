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
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.tuples.Tuple2;

public class TestAsyncCRUDSimpleEntity {

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

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();

    @Test
    public void should_insert_async() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = new Date();
        final SimpleEntity entity = new SimpleEntity(id, date, "value");
        final CountDownLatch latch = new CountDownLatch(1);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(ASYNC_LOGGER_STRING, "%msg - [%thread]%n");

        //When
        manager
                .crud()
                .insert(entity)
                .withResultSetAsyncListener(rs -> {
                    LOGGER.info(CALLED);
                    latch.countDown();
                    return rs;
                })
                .executeAsync();

        //Then
        latch.await();
        logAsserter.assertContains("Called - [achilles-default-executor");

        final List<Row> rows = session.execute("SELECT * FROM simple WHERE id = " + id).all();
        assertThat(rows).hasSize(1);

        final Row row = rows.get(0);
        assertThat(row.getLong("id")).isEqualTo(id);
        assertThat(row.getTimestamp("date")).isEqualTo(date);
        assertThat(row.getString("value")).isEqualTo("value");
    }

    @Test
    public void should_find_by_id_async() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));
        final Date date = buildDateKey();

        final CountDownLatch latch = new CountDownLatch(1);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(ASYNC_LOGGER_STRING, "%msg - [%thread]%n");

        //When
        final CompletableFuture<Tuple2<SimpleEntity, ExecutionInfo>> tuple2 = manager
                .crud()
                .findById(id, date)
                .withResultSetAsyncListener(rs -> {
                    LOGGER.info(CALLED);
                    latch.countDown();
                    return rs;
                })
                .getAsyncWithStats();

        //Then
        latch.await();
        final SimpleEntity actual = tuple2.get()._1();
        final ExecutionInfo executionInfo = tuple2.get()._2();

        assertThat(actual).isNotNull();
        assertThat(actual.getConsistencyList()).containsExactly(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_ONE);
        assertThat(executionInfo.getQueriedHost().isUp()).isTrue();

        logAsserter.assertContains("Called - [achilles-default-executor");
    }

    @Test
    public void should_delete_instance_async() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final SimpleEntity entity = new SimpleEntity(id, date, "value");
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final CountDownLatch latch = new CountDownLatch(1);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(ASYNC_LOGGER_STRING, "%msg - [%thread]%n");

        //When
        final CompletableFuture<ExecutionInfo> future = manager.crud()
                .delete(entity)
                .withResultSetAsyncListener(rs -> {
                    LOGGER.info(CALLED);
                    latch.countDown();
                    return rs;
                })
                .executeAsyncWithStats();

        //Then
        latch.await();
        logAsserter.assertContains("Called");
        final List<Row> rows = session.execute("SELECT * FROM simple WHERE id = " + id).all();
        assertThat(rows).isEmpty();

        final ExecutionInfo executionInfo = future.get();
        assertThat(executionInfo.getQueriedHost().isUp()).isTrue();
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
