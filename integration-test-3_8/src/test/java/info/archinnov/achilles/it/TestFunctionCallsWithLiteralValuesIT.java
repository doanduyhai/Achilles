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

import static info.archinnov.achilles.generated.meta.entity.EntityForAggregate_AchillesMeta.COLUMNS;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_8;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_8;
import info.archinnov.achilles.generated.function.Double_Type;
import info.archinnov.achilles.generated.function.FunctionsRegistry;
import info.archinnov.achilles.generated.function.String_Type;
import info.archinnov.achilles.generated.function.SystemFunctions;
import info.archinnov.achilles.generated.manager.EntityForAggregate_Manager;
import info.archinnov.achilles.generated.meta.entity.EntityForAggregate_AchillesMeta;
import info.archinnov.achilles.internals.entities.EntityForAggregate;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;

public class TestFunctionCallsWithLiteralValuesIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_8> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_8")
            .withScript("functions/createFunctions.cql")
            .entityClassesToTruncate(EntityForAggregate.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_8
                    .builder(cluster)
                    .withDefaultKeyspaceName("it_3_8")
                    .withManagedEntityClasses(EntityForAggregate.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityForAggregate_Manager manager = resource.getManagerFactory().forEntityForAggregate();

    @Test
    public void should_call_textToLong_with_literal_value() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForAggregate/insert_single_row.cql", ImmutableMap.of("id", id));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .function(FunctionsRegistry.textToLong(String_Type.wrap("12")), "longval")
                .fromBaseTable()
                .without_WHERE_Clause()
                .limit(1)
                .getTypedMap();

        //Then
        assertThat(typedMap).hasSize(1);
        assertThat(typedMap.<Long>getTyped("longval")).isEqualTo(12L);
    }

    @Test
    public void should_chain_function_calls_with_literal_value() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForAggregate/insert_single_row.cql", ImmutableMap.of("id", id));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .function(SystemFunctions.bigintAsBlob(FunctionsRegistry.textToLong(String_Type.wrap("12"))), "blobval")
                .fromBaseTable()
                .without_WHERE_Clause()
                .limit(1)
                .getTypedMap();

        //Then
        assertThat(typedMap).hasSize(1);
        assertThat(typedMap).containsKey("blobval");
        final byte[] expected={0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0c};
        assertThat(typedMap.<ByteBuffer>getTyped("blobval").array()).isEqualTo(expected);
    }

    @Test
    public void should_call_aggregate_with_literal_values() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForAggregate/insert_rows.cql", ImmutableMap.of("id", id));


        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .function(FunctionsRegistry.findByDoubleValue(COLUMNS.CLUSTERING, COLUMNS.STRING_VAL, COLUMNS.DOUBLE_VAL,
                        Double_Type.wrap(2.0), Double_Type.wrap(4.0)), "map_result")
                .fromBaseTable()
                .without_WHERE_Clause()
                .limit(1)
                .getTypedMap();

        //Then
        assertThat(typedMap).hasSize(1);
        assertThat(typedMap).containsKey("map_result");
        Map<Integer,String> mapResult = typedMap.getTyped("map_result");

        assertThat(mapResult).hasSize(3);
        assertThat(mapResult).containsEntry(2, "2.0");
        assertThat(mapResult).containsEntry(3, "3.0");
        assertThat(mapResult).containsEntry(4, "4.0");
    }
}
