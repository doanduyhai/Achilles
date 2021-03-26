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
import static info.archinnov.achilles.generated.function.SystemFunctions.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.function.FunctionsRegistry;
import info.archinnov.achilles.generated.manager.EntityWithComplexTypes_Manager;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.generated.meta.entity.EntityWithComplexTypes_AchillesMeta;
import info.archinnov.achilles.generated.meta.entity.SimpleEntity_AchillesMeta;
import info.archinnov.achilles.internals.codecs.EncodingOrdinalCodec;
import info.archinnov.achilles.internals.codecs.ProtocolVersionCodec;
import info.archinnov.achilles.internals.entities.EntityWithComplexTypes;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.internals.entities.TestUDT;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.tuples.Tuple3;

public class TestFunctionCallsSimpleEntity {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class, EntityWithComplexTypes.class)
            .truncateBeforeAndAfterTest()
            .withScript("functions/createFunctions.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(SimpleEntity.class, EntityWithComplexTypes.class)
                .doForceSchemaCreation(true)
                .withStatementsCache(statementsCache)
                .withRuntimeCodec(new CodecSignature<>(ProtocolVersion.class, String.class),
                        new ProtocolVersionCodec())
                .withRuntimeCodec(new CodecSignature<>(Enumerated.Encoding.class, Integer.class, "encoding_codec"),
                        new EncodingOrdinalCodec())
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
            .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();
    private EntityWithComplexTypes_Manager complexTypes_manager = resource.getManagerFactory().forEntityWithComplexTypes();

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
                .function(token(SimpleEntity_AchillesMeta.COLUMNS.ID), "tokens")
                .function(token(SimpleEntity_AchillesMeta.COLUMNS.PARTITION_KEYS), "partitionTokens")
                .function(toUnixTimestamp(SimpleEntity_AchillesMeta.COLUMNS.DATE), "dateAsLong")
                .function(writetime(SimpleEntity_AchillesMeta.COLUMNS.VALUE), "writetimeOfValue")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("tokens")).isNotNull();
        assertThat(typedMap.<Long>getTyped("partitiontokens")).isNotNull();
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
                .function(max(writetime(SimpleEntity_AchillesMeta.COLUMNS.VALUE)), "maxWritetimeOfValue")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("maxwritetimeofvalue")).isGreaterThan(date.getTime());
    }

    @Test
    public void should_dsl_with_udf_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(FunctionsRegistry.convertConsistencyLevelList(SimpleEntity_AchillesMeta.COLUMNS.CONSISTENCY_LIST), "consistency_levels")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<String>getTyped("consistency_levels")).isEqualTo("['QUORUM','LOCAL_ONE']");
    }

    @Test
    public void should_fail_call_writetime_on_another_function_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Date date = buildDateKey();
        scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        exception.expect(AchillesException.class);
        exception.expectMessage("Invalid argument for 'writetime' function, it does not accept function call as argument, only simple column");

        //When
        manager
                .dsl()
                .select()
                .id()
                .function(writetime(max(SimpleEntity_AchillesMeta.COLUMNS.VALUE)), "maxWritetimeOfValue")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .date().Eq(date)
                .getTypedMap();

    }

    @Test
    public void should_call_user_defined_functions() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final TestUDT udt = new TestUDT();
        final EntityWithComplexTypes entity = new EntityWithComplexTypes();

        entity.setId(id);
        entity.setValue("12345");
        entity.setListOfOptional(Arrays.asList(Optional.of("one"), Optional.of("two")));
        entity.setComplexNestingMap(ImmutableMap.of(udt,
                ImmutableMap.of(1, Tuple3.of(1, 2, ConsistencyLevel.ALL))));

        complexTypes_manager.crud().insert(entity).execute();

        //When
        final TypedMap typedMap = complexTypes_manager.dsl()
                .select()
                .function(FunctionsRegistry.convertToLong(EntityWithComplexTypes_AchillesMeta.COLUMNS.VALUE), "asLong")
                .function(FunctionsRegistry.convertListToJson(EntityWithComplexTypes_AchillesMeta.COLUMNS.LIST_OF_OPTIONAL), "list_as_json")
                .function(FunctionsRegistry.stringifyComplexNestingMap(EntityWithComplexTypes_AchillesMeta.COLUMNS.COMPLEX_NESTING_MAP), "complex_map")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getTypedMap();

        //Then
        assertThat(typedMap.<Long>getTyped("aslong")).isEqualTo(12345L);
        assertThat(typedMap.<String>getTyped("list_as_json")).isEqualTo("[one, two]");
        //TODO implement method once https://issues.apache.org/jira/browse/CASSANDRA-11391 is solved
        assertThat(typedMap.<String>getTyped("complex_map")).contains("whatever");
    }

    private Date buildDateKey() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.parse("2015-10-01 00:00:00 GMT");
    }
}
