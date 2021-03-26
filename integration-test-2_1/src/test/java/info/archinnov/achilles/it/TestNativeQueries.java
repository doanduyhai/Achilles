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

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;

public class TestNativeQueries {

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
    private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    @Test
    public void should_perform_regular_native_query() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final SimpleStatement statement = new SimpleStatement("SELECT * FROM simple WHERE id = " + id);

        //When
        final TypedMap actual = manager
                .raw()
                .nativeQuery(statement)
                .getTypedMap();

        assertThat(actual).isNotNull();
        assertThat(actual.<String>getTyped("value")).contains("0 AM");
    }

    @Test
    public void should_perform_prepared_native_query() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        final PreparedStatement preparedStatement = session.prepare("SELECT * FROM simple WHERE id = :id");

        //When
        final TypedMap actual = manager
                .raw()
                .nativeQuery(preparedStatement, id)
                .getTypedMap();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.<String>getTyped("value")).contains("0 AM");
    }

    @Test
    public void should_perform_bound_statement_native_query() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final BoundStatement boundStatement = session.prepare("SELECT * FROM simple WHERE id = :id").bind(id);

        //Then
        final TypedMap actual = manager
                .raw()
                .nativeQuery(boundStatement)
                .getTypedMap();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.<String>getTyped("value")).contains("0 AM");
    }

    @Test
    public void should_iterate_regular_typed_query() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
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

        final SimpleStatement statement = new SimpleStatement("SELECT * FROM simple WHERE id = :id LIMIT 100");

        //When
        final Iterator<TypedMap> iter = manager
                .raw()
                .nativeQuery(statement, id)
                .typedMapIterator();


        //Then
        final AtomicBoolean foundEntity = new AtomicBoolean(false);
        iter.forEachRemaining(instance -> {
            foundEntity.getAndSet(true);
            assertThat(instance).isNotNull();
            assertThat(instance.<String>getTyped("value")).contains("id - date");
        });
        assertThat(foundEntity.get()).isTrue();
    }

    @Test
    public void should_perform_regular_insert_as_native_query() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final SimpleStatement statement = new SimpleStatement("INSERT INTO simple(id, date, value) VALUES(:id, :date, :value)");

        //When
        manager
                .raw()
                .nativeQuery(statement, id, new Date(), "val")
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM simple WHERE id = " + id).one();
        assertThat(actual).isNotNull();
        assertThat(actual.getString("value")).isEqualTo("val");
    }

    @Test
    public void should_limit_displayed_returned_results() throws Exception {
        //Given
        final Map<String, Object> values = new HashMap<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        values.put("id", id);
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

        final SimpleStatement statement = new SimpleStatement("SELECT * FROM simple WHERE id = :id LIMIT 100");

        CassandraLogAsserter logAsserter = new CassandraLogAsserter();

        logAsserter.prepareLogLevel(SimpleEntity.class.getCanonicalName());

        //When
        final List<TypedMap> typedMaps = manager.raw()
                .nativeQuery(statement, id)
                .withDMLResultsDisplaySize(2)
                .getTypedMaps();

        //Then
        try {
            assertThat(typedMaps).hasSize(9);

            logAsserter.assertNotContains("value: id - date3");
        } finally {

        }
    }
}
