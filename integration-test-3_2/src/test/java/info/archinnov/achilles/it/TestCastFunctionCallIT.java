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
import static info.archinnov.achilles.generated.function.SystemFunctions.castAsText;
import static info.archinnov.achilles.generated.function.SystemFunctions.writetime;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_2;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_2;
import info.archinnov.achilles.generated.function.FunctionsRegistry;
import info.archinnov.achilles.generated.manager.EntityForCastFunctionCall_Manager;
import info.archinnov.achilles.generated.meta.entity.EntityForCastFunctionCall_AchillesMeta;
import info.archinnov.achilles.internals.entities.EntityForCastFunctionCall;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;

public class TestCastFunctionCallIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_2> resource = AchillesTestResourceBuilder
            .forJunit()
            .withScript("functions/createFunctions3_2.cql")
            .entityClassesToTruncate(EntityForCastFunctionCall.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_2
                    .builder(cluster)
                    .withManagedEntityClasses(EntityForCastFunctionCall.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityForCastFunctionCall_Manager manager = resource.getManagerFactory().forEntityForCastFunctionCall();

    @Test
    public void should_dsl_with_cast_nested_into_udf_call() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        Date now = new Date();
        Thread.sleep(2);
        scriptExecutor.executeScriptTemplate("EntityForCastFunctionCall/insertRow.cql", ImmutableMap.of("id", id));


        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(FunctionsRegistry.convertStringToLong(castAsText(writetime(EntityForCastFunctionCall_AchillesMeta.COLUMNS.VALUE))), "casted")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("casted")).isGreaterThan(now.getTime());
    }

    @Test
    public void should_dsl_with_nested_system_casting_call() throws Exception {
        //Given
        Date now = new Date();
        Thread.sleep(2);
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForCastFunctionCall/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(castAsText(writetime(EntityForCastFunctionCall_AchillesMeta.COLUMNS.VALUE)), "casted")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<String>getTyped("casted").compareTo(now.getTime() + "")).isGreaterThan(0);
    }



}
