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
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableMap;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithClusteringColumns_Manager;
import info.archinnov.achilles.internals.entities.EntityWithClusteringColumns;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestDSLEntityWithClusterings {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithClusteringColumns.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithClusteringColumns.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithClusteringColumns_Manager manager = resource.getManagerFactory().forEntityWithClusteringColumns();

    @Test
    public void should_dsl_select_one() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final Date date = buildDateKey();

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        final EntityWithClusteringColumns actual = manager
                .dsl()
                .select()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .date().Eq(date)
                .getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getValue()).isEqualTo("val");
    }

    @Test
    public void should_dsl_select_slice_with_tuples_same_partition() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id);
        values.put("id2", id);
        values.put("id3", id);
        values.put("id4", id);
        values.put("id5", id);

        final UUID uuid1 = new UUID(0L, 0L);
        final UUID uuid2 = new UUID(0L, 1L);

        values.put("uuid1", uuid1);
        values.put("uuid2", uuid1);
        values.put("uuid3", uuid1);
        values.put("uuid4", uuid2);
        values.put("uuid5", uuid2);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        final Date date2 = dateFormat.parse("2015-10-02 00:00:00 GMT");
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");
        final Date date4 = dateFormat.parse("2015-10-04 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");

        /*
            Data are ordered as physically:

            uuid1, date3,
            uuid1, date2,
            uuid1, date1,
            uuid2, date5,
            uuid2, date4

            because date is ORDERED BY DESC natively

            but (uuid,date) >= (uuid1, date2) AND (uuid,date) < (uuid2, date4) should return

            uuid1, date3
            uuid1, date2

         */
        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);


        //When
        List<EntityWithClusteringColumns> list = manager
                .dsl()
                .select()
                .uuid()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid_date().Gte_And_Lt(uuid1, date2, uuid2, date4)
                .getList();

        //Then
        assertThat(list).hasSize(2);

        assertThat(list.get(0).getUuid()).isEqualTo(uuid1);
        assertThat(list.get(0).getDate()).isEqualTo(date3);
        assertThat(list.get(0).getValue()).isEqualTo("val3");

        assertThat(list.get(1).getUuid()).isEqualTo(uuid1);
        assertThat(list.get(1).getDate()).isEqualTo(date2);
        assertThat(list.get(1).getValue()).isEqualTo("val2");

        //When
        list = manager
                .dsl()
                .select()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid_date().Gte(uuid2, date4)
                .getList();

        assertThat(list).hasSize(2);
    }


    @Test
    public void should_dsl_select_slice_with_asymetric_tuples_same_partition() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id);
        values.put("id2", id);
        values.put("id3", id);
        values.put("id4", id);
        values.put("id5", id);

        final UUID uuid1 = new UUID(0L, 0L);
        final UUID uuid2 = new UUID(0L, 1L);

        values.put("uuid1", uuid1);
        values.put("uuid2", uuid1);
        values.put("uuid3", uuid1);
        values.put("uuid4", uuid2);
        values.put("uuid5", uuid2);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date2 = dateFormat.parse("2015-10-02 00:00:00 GMT");
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        /*
            Data are ordered as:

            uuid1, date3,
            uuid1, date2,
            uuid1, date1,
            uuid2, date5,
            uuid2, date4

            because date is ORDERED BY DESC natively

            but (uuid,date) > (uuid1, date2) AND uuid < uuid2 should return

            uuid1, date3

         */

        //When
        final List<EntityWithClusteringColumns> list = manager
                .dsl()
                .select()
                .uuid()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid_date().uuid_And_date_Gt_And_uuid_Lt(uuid1, date2, uuid2)
                .getList();

        //Then
        assertThat(list).hasSize(1);

        assertThat(list.get(0).getUuid()).isEqualTo(uuid1);
        assertThat(list.get(0).getDate()).isEqualTo(date3);
        assertThat(list.get(0).getValue()).isEqualTo("val3");
    }

    @Test
    public void should_dsl_select_slice_with_clustering_IN_same_partition() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id);
        values.put("id2", id);
        values.put("id3", id);
        values.put("id4", id);
        values.put("id5", id);

        final UUID uuid1 = new UUID(0L, 0L);
        final UUID uuid2 = new UUID(0L, 1L);

        values.put("uuid1", uuid1);
        values.put("uuid2", uuid1);
        values.put("uuid3", uuid1);
        values.put("uuid4", uuid2);
        values.put("uuid5", uuid2);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");
        final Date date5 = dateFormat.parse("2015-10-05 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        /*
            Data are ordered as:

            uuid1, date3,
            uuid1, date2,
            uuid1, date1,
            uuid2, date5,
            uuid2, date4

            because date is ORDERED BY DESC natively
         */

        //When
        final List<EntityWithClusteringColumns> list = manager
                .dsl()
                .select()
                .uuid()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().IN(uuid1, uuid2)
                .date().Gte_And_Lte(date3, date5)
                .getList();

        //Then
        assertThat(list).hasSize(3);


        assertThat(list
                        .stream()
                        .map(EntityWithClusteringColumns::getValue)
                        .sorted()
                        .collect(toList())
        ).containsExactly("val3", "val4", "val5");
    }

    @Test
    public void should_dsl_select_slice_with_clusterings_IN_same_partition() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id);
        values.put("id2", id);
        values.put("id3", id);
        values.put("id4", id);
        values.put("id5", id);

        final UUID uuid1 = new UUID(0L, 0L);
        final UUID uuid2 = new UUID(0L, 1L);

        values.put("uuid1", uuid1);
        values.put("uuid2", uuid1);
        values.put("uuid3", uuid1);
        values.put("uuid4", uuid2);
        values.put("uuid5", uuid2);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");
        final Date date5 = dateFormat.parse("2015-10-05 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        /*
            Data are ordered as:

            uuid1, date3,
            uuid1, date2,
            uuid1, date1,
            uuid2, date5,
            uuid2, date4

            because date is ORDERED BY DESC natively
         */

        //When
        final List<EntityWithClusteringColumns> list = manager
                .dsl()
                .select()
                .uuid()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().IN(uuid1, uuid2)
                .date().IN(date3, date5)
                .getList();

        //Then
        assertThat(list).hasSize(2);


        assertThat(list
                        .stream()
                        .map(EntityWithClusteringColumns::getValue)
                        .sorted()
                        .collect(toList())
        ).containsExactly("val3", "val5");
    }

    @Test
    public void should_dsl_select_slice_with_different_partitions() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        values.put("id1", id1);
        values.put("id2", id1);
        values.put("id3", id1);
        values.put("id4", id2);
        values.put("id5", id2);

        final UUID uuid = new UUID(0L, 0L);

        values.put("uuid1", uuid);
        values.put("uuid2", uuid);
        values.put("uuid3", uuid);
        values.put("uuid4", uuid);
        values.put("uuid5", uuid);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date3 = dateFormat.parse("2015-10-03 00:00:00 GMT");
        final Date date5 = dateFormat.parse("2015-10-05 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        /*
            Data are ordered as:

            id1, uuid, date3,
            id1, uuid, date2,
            id1, uuid, date1,
            id2, uuid, date5,
            id2, uuid, date4

            because date is ORDERED BY DESC natively
         */

        //When
        final List<EntityWithClusteringColumns> list = manager
                .dsl()
                .select()
                .uuid()
                .date()
                .value()
                .fromBaseTable()
                .where()
                .id().IN(id1, id2)
                .uuid().Eq(uuid)
                .date().IN(date3, date5)
                .getList();

        //Then
        assertThat(list).hasSize(2);


        assertThat(list
                        .stream()
                        .map(EntityWithClusteringColumns::getValue)
                        .sorted()
                        .collect(toList())
        ).containsExactly("val3", "val5");
    }

    @Test
    public void should_dsl_select_slice_with_displayed_results_max() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id1);
        values.put("id2", id1);
        values.put("id3", id1);
        values.put("id4", id2);
        values.put("id5", id2);

        final UUID uuid = new UUID(0L, 0L);

        values.put("uuid1", uuid);
        values.put("uuid2", uuid);
        values.put("uuid3", uuid);
        values.put("uuid4", uuid);
        values.put("uuid5", uuid);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-01 00:00:00+0000'");
        values.put("date5", "'2015-10-02 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevel(EntityWithClusteringColumns.class.getCanonicalName());
        Logger logger = (Logger) LoggerFactory.getLogger(EntityWithClusteringColumns.class);
        logger.setLevel(Level.DEBUG);

        final List<EntityWithClusteringColumns> found = manager.dsl()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id1)
                .withDMLResultsDisplaySize(2)
                .getList();

        try {
            assertThat(found).hasSize(3);

            logAsserter.assertNotContains("val1");
        } finally {
            logger.setLevel(Level.WARN);
        }
    }

    @Test
    public void should_dsl_update_multiple_partitions() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id1);
        values.put("id2", id1);
        values.put("id3", id1);
        values.put("id4", id2);
        values.put("id5", id2);

        final UUID uuid = new UUID(0L, 0L);

        values.put("uuid1", uuid);
        values.put("uuid2", uuid);
        values.put("uuid3", uuid);
        values.put("uuid4", uuid);
        values.put("uuid5", uuid);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date1 = dateFormat.parse("2015-10-01 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-01 00:00:00+0000'");
        values.put("date5", "'2015-10-02 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        /*
            Data are ordered as:

            id1, uuid, date3,
            id1, uuid, date2,
            id1, uuid, date1,
            id2, uuid, date2,
            id2, uuid, date1

            because date is ORDERED BY DESC natively
         */

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new_value")
                .where()
                .id().IN(id1, id2)
                .uuid().Eq(uuid)
                .date().Eq(date1)
                .execute();


        //Then
        final List<Row> actuals = session.execute("SELECT value FROM entity_with_clusterings WHERE id IN (" + id1 + "," + id2
                + ") AND uuid = " + uuid
                + " AND date = '2015-10-01 00:00:00+0000'").all();

        assertThat(actuals).hasSize(2);
        assertThat(actuals.get(0).getString("value")).isEqualTo("new_value");
        assertThat(actuals.get(1).getString("value")).isEqualTo("new_value");
    }

    @Test
    public void should_dsl_delete_multiple_partitions() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id1", id);
        values.put("id2", id);
        values.put("id3", id);
        values.put("id4", id);
        values.put("id5", id);

        final UUID uuid = new UUID(0L, 0L);

        values.put("uuid1", uuid);
        values.put("uuid2", uuid);
        values.put("uuid3", uuid);
        values.put("uuid4", uuid);
        values.put("uuid5", uuid);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        final Date date2 = dateFormat.parse("2015-10-02 00:00:00 GMT");

        values.put("date1", "'2015-10-01 00:00:00+0000'");
        values.put("date2", "'2015-10-02 00:00:00+0000'");
        values.put("date3", "'2015-10-03 00:00:00+0000'");
        values.put("date4", "'2015-10-04 00:00:00+0000'");
        values.put("date5", "'2015-10-05 00:00:00+0000'");

        scriptExecutor.executeScriptTemplate("EntityWithClusteringColumns/insert_many_rows.cql", values);

        /*
            Data are ordered as:

            uuid, date5,
            uuid, date4,
            uuid, date3,
            uuid, date2,
            uuid, date1

            because date is ORDERED BY DESC natively
         */

        //When
        manager
                .dsl()
                .delete()
                .value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .date().Eq(date2)
                .execute();


        //Then
        final List<Row> actuals = session.execute("SELECT value FROM entity_with_clusterings WHERE id = " + id
                + " AND uuid = " + uuid).all();

        assertThat(actuals).hasSize(5);
        assertThat(actuals.get(0).getString("value")).isEqualTo("val5");
        assertThat(actuals.get(1).getString("value")).isEqualTo("val4");
        assertThat(actuals.get(2).getString("value")).isEqualTo("val3");
        assertThat(actuals.get(3).isNull("value")).isTrue();
        assertThat(actuals.get(4).getString("value")).isEqualTo("val1");
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
