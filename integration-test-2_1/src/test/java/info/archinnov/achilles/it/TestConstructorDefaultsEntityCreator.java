/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.ImmutableDefaultsEntity_Manager;
import info.archinnov.achilles.internals.dsl.crud.DeleteByPartitionWithOptions;
import info.archinnov.achilles.internals.dsl.crud.DeleteWithOptions;
import info.archinnov.achilles.internals.dsl.crud.InsertWithOptions;
import info.archinnov.achilles.internals.entities.ImmutableDefaultsEntity;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.tuples.Tuple2;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.THREE;
import static com.datastax.driver.core.ConsistencyLevel.TWO;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestConstructorDefaultsEntityCreator {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(ImmutableDefaultsEntity.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(ImmutableDefaultsEntity.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private ImmutableDefaultsEntity_Manager manager = resource.getManagerFactory().forImmutableDefaultsEntity();

    @Test
    public void should_insert() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = new Date();
        final ImmutableDefaultsEntity entity = newInstance(id, date);

        //When
        manager.crud().insert(entity).execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).all();
        assertThat(rows).hasSize(1);

        final Row row = rows.get(0);
        assertThat(row.getLong("id")).isEqualTo(id);
        assertThat(row.getTimestamp("date")).isEqualTo(date);
        assertThat(row.getString("value")).isEqualTo("value");
        assertThat(row.getList("consistencylist", String.class)).containsExactly("EACH_QUORUM", "LOCAL_QUORUM");
        assertThat(row.getSet("simpleset", Double.class)).containsOnly(10d, 11d);
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(1, "one");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(2, "two");
    }

    @Test
    public void should_insert_with_execution_info() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = new Date();
        final ImmutableDefaultsEntity entity = newInstance(id, date);

        //When
        final ExecutionInfo executionInfo = manager.crud().insert(entity).executeWithStats();

        //Then
        assertThat(executionInfo).isNotNull();
        assertThat(executionInfo.getQueriedHost().isUp()).isTrue();
    }

    @Test
    public void should_insert_if_not_exists() throws Exception {
        //Given
        final long id = 100L;
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        final ImmutableDefaultsEntity entity = newInstance(id, date);
        final AtomicBoolean error = new AtomicBoolean(false);
        final AtomicLong currentId = new AtomicLong(0L);

        final LWTResultListener lwtListener = new LWTResultListener() {

            @Override
            public void onSuccess() {

            }

            @Override
            public void onError(LWTResult lwtResult) {
                error.getAndSet(true);
                currentId.getAndSet(lwtResult.currentValues().getTyped("id"));
            }
        };

        //When
        manager.crud().insert(entity).ifNotExists().withLwtResultListener(lwtListener).execute();

        //Then
        assertThat(error.get()).isTrue();
        assertThat(currentId.get()).isEqualTo(id);
    }

    @Test
    public void should_insert_with_timestamp() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final ImmutableDefaultsEntity entity = newInstance(id, date);


        //When
        manager
                .crud()
                .insert(entity)
                .usingTimestamp(1000L)
                .execute();

        //Then
        final Row row = session.execute("SELECT writetime(value) as wt FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getLong("wt")).isEqualTo(1000L);
    }

    @Test
    public void should_insert_with_ttl() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final ImmutableDefaultsEntity entity = newInstance(id, date);


        //When
        manager
                .crud()
                .insert(entity)
                .usingTimeToLive(1)
                .execute();

        Thread.sleep(1001);

        //Then
        final Row row = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNull();
    }

    @Test
    public void should_insert_generate_query_and_bound_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, "value", Arrays.asList(ALL), null, null);

        //When
        final InsertWithOptions<ImmutableDefaultsEntity> insert = manager
                .crud()
                .insert(entity)
                .usingTimeToLive(123)
                .usingTimestamp(100L);

        //Then

        String expectedQuery = "INSERT INTO " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".immutable_defaults_value (id,date,consistencylist,simplemap,simpleset,value) " +
                "VALUES (:id,:date,:consistencylist,:simplemap,:simpleset,:value) " +
                "USING TTL :ttl;";

        assertThat(insert.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(insert.getBoundValues()).containsExactly(id, date, Arrays.asList(ALL), null, null, "value", 123);
        assertThat(insert.getEncodedBoundValues()).containsExactly(id, date, Arrays.asList("ALL"), null, null, "value", 123);
        assertThat(insert.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_insert_with_downgrading_consistency() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, "value", emptyList(), emptySet(), emptyMap());

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        manager
                .crud()
                .insert(entity)
                .withConsistencyLevel(TWO)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNotNull();
        logAsserter.assertConsistencyLevels(TWO, ONE);
    }

    @Test
    public void should_insert_with_insert_strategy_non_null_fields() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));
        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, null, emptyList(), emptySet(), emptyMap());

        //When
        manager
                .crud()
                .insert(entity)
                .withInsertStrategy(InsertStrategy.NOT_NULL_FIELDS)
                .execute();

        //Then
        final Row row = session.execute("SELECT value FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("0 AM");
    }

    @Test
    public void should_insert_with_schema_name_provider() throws Exception {
        //Given
        final String tableName = "simple_insert_with_schema_name";
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/create_simple_mirror_table.cql", ImmutableMap.of("table", tableName));
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, "value_tenant3", emptyList(), emptySet(), emptyMap());

        final SchemaNameProvider provider = new SchemaNameProvider() {
            @Override
            public <T> String keyspaceFor(Class<T> entityClass) {
                return DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
            }

            @Override
            public <T> String tableNameFor(Class<T> entityClass) {
                return tableName;
            }
        };

        //When
        manager.crud().withSchemaNameProvider(provider).insert(entity).execute();

        //Then
        final Row row = session.execute("SELECT * FROM " + tableName + " WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("value_tenant3");
    }

    @Test
    public void should_find_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));
        final Date date = buildDateKey();

        //When
        final ImmutableDefaultsEntity actual = manager.crud().findById(id, date).get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getConsistencyList()).containsExactly(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_ONE);
        assertThat(actual.getSimpleSet()).containsExactly(1.0, 2.0);
        assertThat(actual.getSimpleMap()).containsEntry(10, "ten");
        assertThat(actual.getSimpleMap()).containsEntry(20, "twenty");
    }

    @Test
    public void should_find_by_id_with_execution_info() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));
        final Date date = buildDateKey();

        //When
        final Tuple2<ImmutableDefaultsEntity, ExecutionInfo> tuple2 = manager.crud().findById(id, date)
                .withConsistencyLevel(TWO)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .getWithStats();

        //Then
        final ExecutionInfo executionInfo = tuple2._2();
        assertThat(executionInfo).isNotNull();
        assertThat(executionInfo.getAchievedConsistencyLevel()).isEqualTo(ONE);
        assertThat(executionInfo.getQueriedHost().isUp()).isTrue();
    }

    @Test
    public void should_find_with_async_listeners() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        //When
        final ImmutableDefaultsEntity found = manager
                .crud()
                .findById(id, date)
                .withResultSetAsyncListener(rs -> {
                    assertThat(rs.getAvailableWithoutFetching()).isEqualTo(1);
                    return rs;
                })
                .withRowAsyncListener(row -> {
                    assertThat(row.getLong("id")).isEqualTo(id);
                    return row;
                })
                .get();
        //Then
        assertThat(found).isNotNull();
    }

    @Test
    public void should_find_with_schema_name_provider() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final String tableName = "simple_find_with_schema_name";
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/create_simple_mirror_table.cql", ImmutableMap.of("table", tableName));
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", tableName));

        final SchemaNameProvider provider = new SchemaNameProvider() {
            @Override
            public <T> String keyspaceFor(Class<T> entityClass) {
                return DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
            }

            @Override
            public <T> String tableNameFor(Class<T> entityClass) {
                return tableName;
            }
        };

        //When
        final ImmutableDefaultsEntity actual = manager.crud().withSchemaNameProvider(provider).findById(id, date).get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getValue()).isEqualTo("0 AM");
        assertThat(actual.getConsistencyList()).containsExactly(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_ONE);
        assertThat(actual.getSimpleSet()).containsExactly(1.0, 2.0);
        assertThat(actual.getSimpleMap()).containsEntry(10, "ten");
        assertThat(actual.getSimpleMap()).containsEntry(20, "twenty");
    }

    @Test
    public void should_delete_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        //When
        manager.crud().deleteById(id, date).execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).all();
        assertThat(rows).isEmpty();
    }

    @Test
    public void should_delete_by_id_with_execution_info() throws Exception {
        //Given
        final long id = 300L;
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        //When
        final ExecutionInfo executionInfo = manager.crud()
                .deleteById(id, date)
                .withConsistencyLevel(THREE)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .executeWithStats();

        //Then
        assertThat(executionInfo).isNotNull();
        assertThat(executionInfo.getAchievedConsistencyLevel()).isEqualTo(ONE);
        assertThat(executionInfo.getQueriedHost().isUp()).isTrue();
    }

    @Test
    public void should_delete_by_partition() throws Exception {
        //Given
        final long id = 200L;
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_2_rows_same_partition.cql", ImmutableMap.of("id", id));

        //When
        manager.crud().deleteByPartitionKeys(id).execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).all();
        assertThat(rows).isEmpty();
    }

    @Test
    public void should_delete_if_exists() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final AtomicBoolean error = new AtomicBoolean(false);
        final AtomicBoolean applied = new AtomicBoolean(true);

        final LWTResultListener lwtListener = new LWTResultListener() {

            @Override
            public void onSuccess() {

            }

            @Override
            public void onError(LWTResult lwtResult) {
                error.getAndSet(true);
                applied.getAndSet(lwtResult.currentValues().getTyped("[applied]"));
            }
        };

        //When
        manager.crud().deleteById(id, date).ifExists().withLwtResultListener(lwtListener).execute();

        //Then
        assertThat(error.get()).isTrue();
        assertThat(applied.get()).isFalse();
    }

    @Test
    public void should_delete_with_schema_name_provider() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final String tableName = "simple_delete_with_schema_name";
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/create_simple_mirror_table.cql", ImmutableMap.of("table", tableName));
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", tableName));

        final SchemaNameProvider provider = new SchemaNameProvider() {
            @Override
            public <T> String keyspaceFor(Class<T> entityClass) {
                return DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
            }

            @Override
            public <T> String tableNameFor(Class<T> entityClass) {
                return tableName;
            }
        };

        //When
        manager.crud().withSchemaNameProvider(provider).deleteById(id, date).execute();

        //Then
        final Row row = session.execute("SELECT * FROM " + tableName + " WHERE id = " + id).one();
        assertThat(row).isNull();
    }

    @Test
    public void should_delete_with_equal_condition() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        final AtomicBoolean success = new AtomicBoolean(false);
        final LWTResultListener lwtResultListener = new LWTResultListener() {

            @Override
            public void onSuccess() {
                success.getAndSet(true);
            }

            @Override
            public void onError(LWTResult lwtResult) {

            }
        };
        //When
        manager
                .dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .if_SimpleSet().Eq(Sets.newHashSet(1.0, 2.0))
                .withLwtResultListener(lwtResultListener)
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNull();
        assertThat(success.get()).isTrue();
    }

    @Test
    public void should_delete_with_inequal_condition() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        final AtomicBoolean success = new AtomicBoolean(false);
        final LWTResultListener lwtResultListener = new LWTResultListener() {

            @Override
            public void onSuccess() {
                success.getAndSet(true);
            }

            @Override
            public void onError(LWTResult lwtResult) {

            }
        };
        //When
        manager
                .dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .if_Value().Lt("_")
                .withLwtResultListener(lwtResultListener)
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNull();
        assertThat(success.get()).isTrue();
    }

    @Test
    public void should_delete_with_not_equal_condition() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("ImmutableDefaultsEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "immutable_defaults_value"));

        final AtomicBoolean success = new AtomicBoolean(false);
        final LWTResultListener lwtResultListener = new LWTResultListener() {

            @Override
            public void onSuccess() {
                success.getAndSet(true);
            }

            @Override
            public void onError(LWTResult lwtResult) {

            }
        };
        //When
        manager
                .dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .if_ConsistencyList().NotEq(Arrays.asList(ALL))
                .withLwtResultListener(lwtResultListener)
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM immutable_defaults_value WHERE id = " + id).one();
        assertThat(row).isNull();
        assertThat(success.get()).isTrue();
    }

    @Test
    public void should_delete_entity_generate_query_and_bound_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, "value", Arrays.asList(ALL), emptySet(), emptyMap());

        //When
        final DeleteWithOptions<ImmutableDefaultsEntity> delete = manager
                .crud()
                .delete(entity)
                .usingTimestamp(100L);

        //Then

        String expectedQuery = "DELETE FROM " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".immutable_defaults_value " +
                "WHERE id=:id AND date=:date;";

        assertThat(delete.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(delete.getBoundValues()).containsExactly(id, date);
        assertThat(delete.getEncodedBoundValues()).containsExactly(id, date);
        assertThat(delete.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_delete_by_id_generate_query_and_bound_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, "value", Arrays.asList(ALL), emptySet(), emptyMap());

        //When
        final DeleteWithOptions<ImmutableDefaultsEntity> delete = manager
                .crud()
                .deleteById(id, date)
                .usingTimestamp(100L);

        //Then

        String expectedQuery = "DELETE FROM " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".immutable_defaults_value " +
                "WHERE id=:id AND date=:date;";

        assertThat(delete.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(delete.getBoundValues()).containsExactly(id, date);
        assertThat(delete.getEncodedBoundValues()).containsExactly(id, date);
        assertThat(delete.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_delete_by_partition_generate_query_and_bound_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final ImmutableDefaultsEntity entity = new ImmutableDefaultsEntity(id, date, "value", Arrays.asList(ALL), emptySet(), emptyMap());

        //When
        final DeleteByPartitionWithOptions<ImmutableDefaultsEntity> delete = manager
                .crud()
                .deleteByPartitionKeys(id)
                .usingTimestamp(100L);

        //Then

        String expectedQuery = "DELETE FROM " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".immutable_defaults_value " +
                "WHERE id=:id;";

        assertThat(delete.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(delete.getBoundValues()).containsExactly(id);
        assertThat(delete.getEncodedBoundValues()).containsExactly(id);
        assertThat(delete.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }

    private ImmutableDefaultsEntity newInstance(final long id, final Date date) {
        return new ImmutableDefaultsEntity(
                id, date, "value",
                Arrays.asList(ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
                Sets.newHashSet(10d, 11d),
                ImmutableMap.of(1, "one", 2, "two"));
    }
}
