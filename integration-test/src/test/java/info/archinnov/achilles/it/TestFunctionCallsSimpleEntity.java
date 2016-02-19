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

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static info.archinnov.achilles.generated.function.SystemFunctions.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.function.FunctionsRegistry;
import info.archinnov.achilles.generated.function.SystemFunctions;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;

@RunWith(MockitoJUnitRunner.class)
public class TestFunctionCallsSimpleEntity {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class)
            .truncateBeforeAndAfterTest()
            .withScript("SimpleEntity/createUDF.cql")
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
    public void should_dsl_with_system_function_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(SystemFunctions.toUnixTimestamp(manager.COLUMNS.DATE), "dateAsLong")
                .function(writetime(manager.COLUMNS.VALUE), "writetimeOfValue")
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .date_Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("dateaslong")).isGreaterThan(0L);
        assertThat(typedMap.<Long>getTyped("writetimeofvalue")).isGreaterThan(date.getTime());
    }

    @Test
    public void should_dsl_with_nested_system_function_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(max(writetime(manager.COLUMNS.VALUE)), "maxWritetimeOfValue")
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .date_Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("maxwritetimeofvalue")).isGreaterThan(date.getTime());
    }

    @Test
    public void should_dsl_with_nested_system_casting_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(SystemFunctions.castAsText(SystemFunctions.writetime(manager.COLUMNS.VALUE)), "writetimeOfValueAsString")
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .date_Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(Long.parseLong(typedMap.<String>getTyped("writetimeofvalueasstring"))).isGreaterThan(date.getTime());
    }

    @Test
    public void should_dsl_with_cast_nested_into_udf_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(FunctionsRegistry.convertToLong(castAsText(writetime(manager.COLUMNS.VALUE))), "casted")
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .date_Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("casted")).isGreaterThan(date.getTime());
    }

    @Test
    public void should_dsl_with_complicated_udf_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(FunctionsRegistry.convertListToJson(manager.COLUMNS.CONSISTENCY_LIST), "consistency_levels")
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .date_Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<String>getTyped("consistency_levels")).isEqualTo("['QUORUM','LOCAL_ONE']");
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
