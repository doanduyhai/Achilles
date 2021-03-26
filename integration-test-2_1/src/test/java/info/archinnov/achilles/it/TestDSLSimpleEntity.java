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

import static com.datastax.driver.core.ConsistencyLevel.*;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.dsl.SimpleEntity_Delete;
import info.archinnov.achilles.generated.dsl.SimpleEntity_Select;
import info.archinnov.achilles.generated.dsl.SimpleEntity_Update;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;
import info.archinnov.achilles.type.tuples.Tuple2;

public class TestDSLSimpleEntity {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
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
    public void should_dsl_select_one() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final SimpleEntity actual = manager
                .dsl()
                .select()
                .consistencyList()
                .simpleSet()
                .simpleMap()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getConsistencyList()).containsExactly(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_ONE);
        assertThat(actual.getSimpleSet()).containsExactly(1.0, 2.0);
        assertThat(actual.getSimpleMap()).containsEntry(10, "ten");
        assertThat(actual.getSimpleMap()).containsEntry(20, "twenty");
    }

    @Test
    public void should_dsl_select_with_token_value() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final SimpleEntity actual = manager
                .dsl()
                .select()
                .consistencyList()
                .simpleSet()
                .simpleMap()
                .value()
                .fromBaseTable()
                .where()
                .tokenValueOf_id().Gte_And_Lte(Long.MIN_VALUE, Long.MAX_VALUE)
                .getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getConsistencyList()).containsExactly(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_ONE);
        assertThat(actual.getSimpleSet()).containsExactly(1.0, 2.0);
        assertThat(actual.getSimpleMap()).containsEntry(10, "ten");
        assertThat(actual.getSimpleMap()).containsEntry(20, "twenty");
    }

    @Test
    public void should_dsl_select_slice() throws Exception {
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

        //When
        final List<SimpleEntity> list = manager
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
                .getList();

        //Then
        assertThat(list).hasSize(8);
        final SimpleEntity instance1 = list.get(0);
        final SimpleEntity instance2 = list.get(1);
        final SimpleEntity instance3 = list.get(2);
        final SimpleEntity instance4 = list.get(3);
        final SimpleEntity instance5 = list.get(4);
        final SimpleEntity instance6 = list.get(5);
        final SimpleEntity instance7 = list.get(6);
        final SimpleEntity instance8 = list.get(7);

        assertThat(instance1.getConsistencyList()).containsExactly(ONE);
        assertThat(instance2.getConsistencyList()).containsExactly(TWO);
        assertThat(instance3.getConsistencyList()).containsExactly(THREE);
        assertThat(instance4.getConsistencyList()).containsExactly(QUORUM);
        assertThat(instance5.getConsistencyList()).containsExactly(ALL);
        assertThat(instance6.getConsistencyList()).containsExactly(LOCAL_ONE);
        assertThat(instance7.getConsistencyList()).containsExactly(LOCAL_QUORUM);
        assertThat(instance8.getConsistencyList()).containsExactly(EACH_QUORUM);

        assertThat(instance1.getSimpleSet()).containsOnly(1.0);
        assertThat(instance2.getSimpleSet()).containsOnly(2.0);
        assertThat(instance3.getSimpleSet()).containsOnly(3.0);
        assertThat(instance4.getSimpleSet()).containsOnly(4.0);
        assertThat(instance5.getSimpleSet()).containsOnly(5.0);
        assertThat(instance6.getSimpleSet()).containsOnly(6.0);
        assertThat(instance7.getSimpleSet()).containsOnly(7.0);
        assertThat(instance8.getSimpleSet()).containsOnly(8.0);

        assertThat(instance1.getSimpleMap()).containsExactly(entry(1, "one"));
        assertThat(instance2.getSimpleMap()).containsExactly(entry(2, "two"));
        assertThat(instance3.getSimpleMap()).containsExactly(entry(3, "three"));
        assertThat(instance4.getSimpleMap()).containsExactly(entry(4, "four"));
        assertThat(instance5.getSimpleMap()).containsExactly(entry(5, "five"));
        assertThat(instance6.getSimpleMap()).containsExactly(entry(6, "six"));
        assertThat(instance7.getSimpleMap()).containsExactly(entry(7, "seven"));
        assertThat(instance8.getSimpleMap()).containsExactly(entry(8, "eight"));

        assertThat(instance1.getValue()).isEqualTo("id - date1");
        assertThat(instance2.getValue()).isEqualTo("id - date2");
        assertThat(instance3.getValue()).isEqualTo("id - date3");
        assertThat(instance4.getValue()).isEqualTo("id - date4");
        assertThat(instance5.getValue()).isEqualTo("id - date5");
        assertThat(instance6.getValue()).isEqualTo("id - date6");
        assertThat(instance7.getValue()).isEqualTo("id - date7");
        assertThat(instance8.getValue()).isEqualTo("id - date8");
    }

    @Test
    public void should_dsl_select_slice_with_execution_info() throws Exception {
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

        //When
        final Tuple2<List<SimpleEntity>, ExecutionInfo> tuple2 = manager
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
                .withConsistencyLevel(TWO)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .getListWithStats();

        //Then
        final ExecutionInfo executionInfo = tuple2._2();
        assertThat(executionInfo).isNotNull();
        assertThat(executionInfo.getAchievedConsistencyLevel()).isEqualTo(ONE);
    }

    @Test
    public void should_dsl_select_with_double_IN() throws Exception {
        //Given
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date1 = buildDateKey();
        final Date date2 = new Date();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id1, "table", "simple"));

        //When
        final List<SimpleEntity> actuals = manager
                .dsl()
                .select()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().IN(id1, id2)
                .date().IN(date1, date2)
                .getList();

        //Then
        assertThat(actuals).hasSize(1);
        assertThat(actuals.get(0).getDate()).isEqualTo(date1);
    }

    @Test
    public void should_dsl_select_with_options() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date1 = dateFormat.parse("2015-10-01 00:00:00 GMT");
        final Date date2 = dateFormat.parse("2015-10-02 00:00:00 GMT");
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");
        final Date date6 = dateFormat.parse("2015-10-06 00:00:00 GMT");

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

        final AtomicInteger rsCount = new AtomicInteger(0);
        final AtomicInteger rowCounter = new AtomicInteger(0);

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        final List<SimpleEntity> found = manager
                .dsl()
                .select()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().IN(date1, date2, date3, date6)
                .orderByDateDescending()
                .limit(3)
                .withConsistencyLevel(THREE)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withResultSetAsyncListener(rs -> {
                    rsCount.getAndSet(rs.getAvailableWithoutFetching());
                    return rs;
                })
                .withRowAsyncListener(row -> {
                    rowCounter.getAndIncrement();
                    return row;
                })
                .getList();

        //Then
        assertThat(found).hasSize(3);
        assertThat(found.get(0).getValue()).isEqualTo("id - date6");
        assertThat(found.get(1).getValue()).isEqualTo("id - date3");
        assertThat(found.get(2).getValue()).isEqualTo("id - date2");
        assertThat(rsCount.get()).isEqualTo(3);
        assertThat(rowCounter.get()).isEqualTo(3);
        logAsserter.assertConsistencyLevels(THREE, ONE);
    }

    @Test
    public void should_dsl_select_generate_query_string_and_encoded_values() throws Exception {
        //Given
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date1 = buildDateKey();
        final Date date2 = new Date();

        //When
        final SimpleEntity_Select.E dsl = manager
                .dsl()
                .select()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().IN(id1, id2)
                .date().IN(date1, date2);


        //Then
        final String expectedQuery = "SELECT date,value FROM " +
                DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".simple " +
                "WHERE id IN :id AND date IN :date;";

        assertThat(dsl.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(dsl.getBoundValues()).containsExactly(asList(id1, id2), asList(date1, date2));
        assertThat(dsl.getEncodedBoundValues()).containsExactly(asList(id1, id2), asList(date1, date2));
        assertThat(dsl.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_dsl_select_using_schema_name_provider() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final String tableName = "simple_dsl_schema_provider";
        scriptExecutor.executeScriptTemplate("SimpleEntity/create_simple_mirror_table.cql", ImmutableMap.of("table", tableName));
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", tableName));

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
        final SimpleEntity actual = manager
                .dsl()
                .select()
                .allColumns_From(provider)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getValue()).isEqualTo("0 AM");
    }

    @Test
    public void should_dsl_select_with_iterator() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        final Date date1 = dateFormat.parse("2015-10-01 00:00:00 GMT");
        final Date date2 = dateFormat.parse("2015-10-02 00:00:00 GMT");
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");
        final Date date4 = dateFormat.parse("2015-10-04 00:00:00 GMT");
        final Date date5 = dateFormat.parse("2015-10-05 00:00:00 GMT");
        final Date date6 = dateFormat.parse("2015-10-06 00:00:00 GMT");
        final Date date7 = dateFormat.parse("2015-10-07 00:00:00 GMT");
        final Date date8 = dateFormat.parse("2015-10-08 00:00:00 GMT");
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

        //When
        final Iterator<SimpleEntity> iterator = manager
                .dsl()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .date().Gt_And_Lte(date1, date9)
                .orderByDateDescending()
                .withFetchSize(2)
                .withResultSetAsyncListener(rs -> {
                    assertThat(rs.getAvailableWithoutFetching()).isEqualTo(2);
                    return rs;
                })
                .iterator();

        //Then
        assertThat(iterator.next().getDate()).isEqualTo(date9);
        assertThat(iterator.next().getDate()).isEqualTo(date8);
        assertThat(iterator.next().getDate()).isEqualTo(date7);
        assertThat(iterator.next().getDate()).isEqualTo(date6);
        assertThat(iterator.next().getDate()).isEqualTo(date5);
        assertThat(iterator.next().getDate()).isEqualTo(date4);
        assertThat(iterator.next().getDate()).isEqualTo(date3);
        assertThat(iterator.next().getDate()).isEqualTo(date2);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_dsl_delete() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

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
            .execute();

        //Then
        final Row row = session.execute("SELECT * FROM simple WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.isNull("consistencylist")).isTrue();
        assertThat(row.isNull("simplemap")).isTrue();
        assertThat(row.getSet("simpleset", Double.class)).containsOnly(1.0, 2.0);
        assertThat(row.getString("value")).isEqualTo("0 AM");
    }

    @Test
    public void should_dsl_delete_with_execution_info() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final ExecutionInfo executionInfo = manager
                .dsl()
                .delete()
                .consistencyList()
                .simpleMap()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .withConsistencyLevel(TWO)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .executeWithStats();

        //Then
        assertThat(executionInfo).isNotNull();
        assertThat(executionInfo.getAchievedConsistencyLevel()).isEqualTo(ONE);
    }

    @Test
    public void should_dsl_delete_with_schema_name() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final String tableName = "dsl_delete_with_schema";
        scriptExecutor.executeScriptTemplate("SimpleEntity/create_simple_mirror_table.cql", ImmutableMap.of("table", tableName));
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", tableName));

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
        manager
                .dsl()
                .delete()
                .allColumns_From(provider)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM simple WHERE id = " + id).one();
        assertThat(row).isNull();
    }

    @Test
    public void should_dsl_delete_with_options() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        manager
                .dsl()
                .delete()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .withConsistencyLevel(QUORUM)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .execute();

        //Then
        final Row row = session.execute("SELECT value FROM simple WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.isNull("value")).isTrue();
        logAsserter.assertConsistencyLevels(QUORUM);
    }

    @Test
    public void should_dsl_delete_if_exists() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        final AtomicBoolean error = new AtomicBoolean(false);
        final LWTResultListener lwtResultListener = new LWTResultListener() {

            @Override
            public void onSuccess() {

            }

            @Override
            public void onError(LWTResult lwtResult) {
                error.getAndSet(true);
            }
        };

        //When
        manager
                .dsl()
                .delete()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .ifExists()
                .withResultSetAsyncListener(rs -> {
                    assertThat(rs.wasApplied()).isFalse();
                    return rs;
                })
                .withLwtResultListener(lwtResultListener)
                .execute();
        //Then
        assertThat(error.get()).isTrue();
    }

    @Test
    public void should_dsl_delete_generate_query_string_and_encoded_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        //When
        final SimpleEntity_Delete.E dsl = manager
                .dsl()
                .delete()
                .simpleMap()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date);


        //Then
        final String expectedQuery = "DELETE simplemap,value FROM " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".simple " +
                "WHERE id=:id AND date=:date;";

        assertThat(dsl.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(dsl.getBoundValues()).containsExactly(id, date);
        assertThat(dsl.getEncodedBoundValues()).containsExactly(id, date);
        assertThat(dsl.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_dsl_update_list_append() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().AppendTo(ALL)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("QUORUM", "LOCAL_ONE", "ALL");
    }

    @Test
    public void should_dsl_update_list_appendAll() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().AppendAllTo(asList(TWO, THREE))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("QUORUM", "LOCAL_ONE", "TWO", "THREE");
    }

    @Test
    public void should_dsl_update_list_prepend() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().PrependTo(ALL)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("ALL", "QUORUM", "LOCAL_ONE");
    }

    @Test
    public void should_dsl_update_list_prependAll() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().PrependAllTo(asList(TWO, THREE))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("TWO", "THREE", "QUORUM", "LOCAL_ONE");
    }

    @Test
    public void should_dsl_update_list_removeAtIndex() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().RemoveAtIndex(0)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("LOCAL_ONE");
    }

    @Test
    public void should_dsl_update_list_remove_single() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        scriptExecutor.execute("UPDATE simple SET consistencylist = consistencylist + ['QUORUM', 'QUORUM'] WHERE id = " + id
                + "AND date = '2015-10-01 00:00:00+0000'");

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().RemoveFrom(QUORUM)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("LOCAL_ONE");
    }

    @Test
    public void should_dsl_update_list_removeAll() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().RemoveAllFrom(asList(LOCAL_ONE, QUORUM))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.isNull("consistencylist")).isTrue();
    }

    @Test
    public void should_dsl_update_list_set() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().Set(asList(TWO, THREE))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("TWO", "THREE");
    }

    @Test
    public void should_dsl_update_list_setAtIndex() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .consistencyList().SetAtIndex(0, ONE)
                .consistencyList().SetAtIndex(1, TWO)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT consistencylist FROM simple WHERE id = " + id).one();
        assertThat(row.getList("consistencylist", String.class)).containsExactly("ONE", "TWO");
    }

    @Test
    public void should_dsl_update_set_add() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleSet().AddTo(3.0D)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simpleset FROM simple WHERE id = " + id).one();
        assertThat(row.getSet("simpleset", Double.class)).containsExactly(1.0d, 2.0d, 3.0d);
    }

    @Test
    public void should_dsl_update_set_addAll() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleSet().AddAllTo(Sets.newHashSet(3.0, 4.0))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simpleset FROM simple WHERE id = " + id).one();
        assertThat(row.getSet("simpleset", Double.class)).containsExactly(1.0d, 2.0d, 3.0d, 4.0d);
    }

    @Test
    public void should_dsl_update_set_remove() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleSet().RemoveFrom(1.0d)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simpleset FROM simple WHERE id = " + id).one();
        assertThat(row.getSet("simpleset", Double.class)).containsExactly(2.0d);
    }

    @Test
    public void should_dsl_update_set_removeAll() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));
        scriptExecutor.execute("UPDATE simple SET simpleset = simpleset + {3} WHERE id = " + id
                + "AND date = '2015-10-01 00:00:00+0000'");
        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleSet().RemoveAllFrom(Sets.newHashSet(1.0d, 2.0d))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simpleset FROM simple WHERE id = " + id).one();
        assertThat(row.getSet("simpleset", Double.class)).containsExactly(3.0d);
    }

    @Test
    public void should_dsl_update_set_setValue() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleSet().Set(Sets.newHashSet(3.0))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simpleset FROM simple WHERE id = " + id).one();
        assertThat(row.getSet("simpleset", Double.class)).containsExactly(3.0d);
    }

    @Test
    public void should_dsl_update_map_put() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleMap().PutTo(30, "thirty")
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simplemap FROM simple WHERE id = " + id).one();
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(10, "ten");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(20, "twenty");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(30, "thirty");
    }

    @Test
    public void should_dsl_update_map_removeByKey() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleMap().RemoveByKey(20)
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simplemap FROM simple WHERE id = " + id).one();
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(10, "ten");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).doesNotContainEntry(20, "twenty");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).hasSize(1);
    }

    @Test
    public void should_dsl_update_map_addAll() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleMap().AddAllTo(ImmutableMap.of(20, "new_twenty", 30, "thirty"))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simplemap FROM simple WHERE id = " + id).one();
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(10, "ten");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(20, "new_twenty");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(30, "thirty");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).hasSize(3);
    }

    @Test
    public void should_dsl_update_map_set() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .simpleMap().Set(ImmutableMap.of(20, "new_twenty", 30, "thirty"))
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT simplemap FROM simple WHERE id = " + id).one();
        assertThat(row.getMap("simplemap", Integer.class, String.class)).doesNotContainEntry(10, "ten");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(20, "new_twenty");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).containsEntry(30, "thirty");
        assertThat(row.getMap("simplemap", Integer.class, String.class)).hasSize(2);
    }

    @Test
    public void should_dsl_update_value_if_exists() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final AtomicBoolean error = new AtomicBoolean(false);

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new value")
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .ifExists()
                .withLwtResultListener(new LWTResultListener() {
                    @Override
                    public void onSuccess() {
                    }

                    @Override
                    public void onError(LWTResult lwtResult) {
                        error.getAndSet(true);
                    }
                })
                .withResultSetAsyncListener(rs -> {
                    assertThat(rs.wasApplied()).isFalse();
                    return rs;
                })
                .execute();

        //Then
        final Row row = session.execute("SELECT simplemap FROM simple WHERE id = " + id).one();
        assertThat(row).isNull();
        assertThat(error.get()).isTrue();
    }

    @Test
    public void should_dsl_update_value_with_execution_info() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        //When
        final ExecutionInfo executionInfo = manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new value")
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .withConsistencyLevel(TWO)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .executeWithStats();

        //Then
        assertThat(executionInfo).isNotNull();
        assertThat(executionInfo.getAchievedConsistencyLevel()).isEqualTo(ONE);
    }

    @Test
    public void should_dsl_update_value_if_equal() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final AtomicBoolean success = new AtomicBoolean(false);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

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
                    assertThat(rs.wasApplied()).isTrue();
                    return rs;
                })
                .withSerialConsistencyLevel(SERIAL)
                .execute();

        //Then
        final Row row = session.execute("SELECT value FROM simple WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("new value");
        assertThat(success.get()).isTrue();
        logAsserter.assertSerialConsistencyLevels(SERIAL);
    }

    @Test
    public void should_dsl_update_with_options() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new value")
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .withConsistencyLevel(THREE)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withTracing()
                .execute();

        //Then
        final Row row = session.execute("SELECT value FROM simple WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("new value");
        logAsserter.assertConsistencyLevels(THREE, ONE);
    }

    @Test
    public void should_dsl_update_with_schema_name() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        final String tableName = "update_dsl_with_schema_name";
        scriptExecutor.executeScriptTemplate("SimpleEntity/create_simple_mirror_table.cql", ImmutableMap.of("table", tableName));
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", tableName));

        //When
        manager
                .dsl()
                .update()
                .from(new SchemaNameProvider() {
                    @Override
                    public <T> String keyspaceFor(Class<T> entityClass) {
                        return DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
                    }

                    @Override
                    public <T> String tableNameFor(Class<T> entityClass) {
                        return tableName;
                    }
                })
                .value().Set("new value")
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .execute();

        //Then
        final Row row = session.execute("SELECT value FROM " + tableName + " WHERE id = " + id).one();
        assertThat(row.getString("value")).isEqualTo("new value");
    }

    @Test
    public void should_dsl_update_generate_query_string_and_encoded_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();

        //When
        final SimpleEntity_Update.E dsl = manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new value")
                .consistencyList().AppendTo(ALL)
                .simpleSet().AddTo(3.0)
                .simpleMap().PutTo(30, "thirty")
                .where()
                .id().Eq(id)
                .date().Eq(date);

        //Then
        final String expectedQuery = "UPDATE " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".simple SET value=:value," +
                "consistencylist=consistencylist+:consistencylist," +
                "simpleset=simpleset+:simpleset," +
                "simplemap[:simpleMap_key]=:simpleMap_value " +
                "WHERE id=:id AND date=:date;";

        assertThat(dsl.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(dsl.getBoundValues()).containsExactly("new value", asList(ALL), Sets.newHashSet(3.0), 30, "thirty", id, date);
        assertThat(dsl.getEncodedBoundValues()).containsExactly("new value", asList("ALL"), Sets.newHashSet(3.0), 30, "thirty", id, date);
        assertThat(dsl.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
