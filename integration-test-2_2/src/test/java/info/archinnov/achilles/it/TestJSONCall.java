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
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_2_2;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_2_2;
import info.archinnov.achilles.generated.function.SystemFunctions;
import info.archinnov.achilles.generated.manager.EntityForJSONCall_Manager;
import info.archinnov.achilles.generated.meta.entity.EntityForJSONCall_AchillesMeta;
import info.archinnov.achilles.internals.codecs.EncodingOrdinalCodec;
import info.archinnov.achilles.internals.codecs.ProtocolVersionCodec;
import info.archinnov.achilles.internals.entities.EntityForJSONCall;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;

public class TestJSONCall {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_2_2> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityForJSONCall.class)
            .truncateBeforeAndAfterTest()
            .withScript("functions/createFunctions.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_2_2
                .builder(cluster)
                .withManagedEntityClasses(EntityForJSONCall.class)
                .doForceSchemaCreation(true)
                .withStatementsCache(statementsCache)
                .withRuntimeCodec(new CodecSignature<>(ProtocolVersion.class, String.class),
                        new ProtocolVersionCodec())
                .withRuntimeCodec(new CodecSignature<>(Enumerated.Encoding.class, Integer.class, "encoding_codec"),
                        new EncodingOrdinalCodec())
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
            .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityForJSONCall_Manager manager = resource.getManagerFactory().forEntityForJSONCall();
    private Session session = resource.getNativeSession();

    @Test
    public void should_select_json_star() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForJSONCall/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final String actual = manager
                .dsl()
                .select()
                .allColumnsAsJSON_FromBaseTable()
                .where()
                .id().Eq(id)
                .clust().Eq(1L)
                .getJSON();

        //Then
        assertThat(actual).isEqualTo(format("{\"id\": %s, \"clust\": 1, " +
                "\"liststring\": [\"1\", \"2\"], " +
                "\"mapstring\": {\"1\": \"1\", \"2\": \"2\"}, " +
                "\"setstring\": [\"1\", \"2\"], \"value\": \"val\"}", id));

    }

    @Test
    public void should_select_toJson() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForJSONCall/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final TypedMap actual = manager
                .dsl()
                .select()
                .value()
                .function(SystemFunctions.toJson(EntityForJSONCall_AchillesMeta.COLUMNS.LIST_STRING), "list_as_json")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .clust().Eq(1L)
                .getTypedMap();

        //Then
        assertThat(actual.<String>getTyped("value")).isEqualTo("val");
        assertThat(actual.<String>getTyped("list_as_json")).isEqualTo("[\"1\", \"2\"]");
    }

    @Test
    public void should_select_allJSON() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForJSONCall/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final String json = manager
                .dsl()
                .select()
                .allColumnsAsJSON_FromBaseTable()
                .where()
                .id().Eq(id)
                .getJSON();
        //Then
        assertThat(json).isEqualTo("{\"id\": " + id + ", \"clust\": 1, " +
                "\"liststring\": [\"1\", \"2\"], " +
                "\"mapstring\": {\"1\": \"1\", \"2\": \"2\"}, " +
                "\"setstring\": [\"1\", \"2\"], " +
                "\"value\": \"val\"}");

    }

    @Test
    public void should_update_using_fromJson() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForJSONCall/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .update()
                .fromBaseTable()
                .value().Set_FromJSON("\"new_val\"")
                .listString().Set_FromJSON("[\"one\"]")
                .setString().Set_FromJSON("[\"two\"]")
                .mapString().Set_FromJSON("{\"3\": \"three\"}")
                .where()
                .id().Eq_FromJson("\"" + id + "\"")
                .clust().Eq_FromJson("\"1\"")
                .if_Value().Eq_FromJSON("\"val\"")
                .if_ListString().Eq_FromJSON("[\"1\", \"2\"]")
                .if_SetString().Eq_FromJSON("[\"1\", \"2\"]")
                .if_MapString().Eq_FromJSON("{\"1\": \"1\", \"2\": \"2\"}")
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM achilles_embedded.entity_for_json_function_call WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("new_val");
        assertThat(row.getList("liststring", String.class)).containsExactly("one");
        assertThat(row.getSet("setstring", String.class)).containsExactly("two");
        assertThat(row.getMap("mapstring", Integer.class, String.class)).hasSize(1).containsEntry(3, "three");
    }

    @Test
    public void should_delete_using_fromJson() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForJSONCall/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq_FromJson("\"" + id + "\"")
                .clust().Eq_FromJson("\"1\"")
                .if_Value().Eq_FromJSON("\"val\"")
                .if_ListString().Eq_FromJSON("[\"1\", \"2\"]")
                .if_SetString().Eq_FromJSON("[\"1\", \"2\"]")
                .if_MapString().Eq_FromJSON("{\"1\": \"1\", \"2\": \"2\"}")
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM achilles_embedded.entity_for_json_function_call WHERE id = " + id).one();
        assertThat(row).isNull();
    }

    @Test
    public void should_insert_json() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        //When
        String json = "{\"id\": " + id + ", \"clust\": 1, \"value\": \"val\", " +
                "\"liststring\": [\"one\"], " +
                "\"setstring\": [\"two\"], " +
                "\"mapstring\": {\"3\": \"three\"}" +
                "}";

        manager
                .crud()
                .insertJSON(json)
                .execute();
        //Then
        final Row row = session.execute("SELECT * FROM achilles_embedded.entity_for_json_function_call WHERE id = " + id + "AND clust = 1").one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("val");
        assertThat(row.getList("liststring", String.class)).containsExactly("one");
        assertThat(row.getSet("setstring", String.class)).containsExactly("two");
        assertThat(row.getMap("mapstring", Integer.class, String.class)).hasSize(1).containsEntry(3, "three");
    }

    @Test
    public void should_insert_json_if_not_exists() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        //When
        String json = "{\"id\": " + id + ", \"clust\": 1, \"value\": \"val\", " +
                "\"liststring\": [\"one\"], " +
                "\"setstring\": [\"two\"], " +
                "\"mapstring\": {\"3\": \"three\"}" +
                "}";

        AtomicBoolean success = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        manager
                .crud()
                .insertJSON(json)
                .ifNotExists()
                .withLwtResultListener(new LWTResultListener() {

                    @Override
                    public void onSuccess() {
                        success.getAndSet(true);
                        latch.countDown();
                    }

                    @Override
                    public void onError(LWTResult lwtResult) {
                        latch.countDown();
                    }
                })
                .execute();

        //Then
        latch.await();
        assertThat(success.get()).isTrue();
        final Row row = session.execute("SELECT * FROM achilles_embedded.entity_for_json_function_call WHERE id = " + id + "AND clust = 1").one();
        assertThat(row).isNotNull();
        assertThat(row.getString("value")).isEqualTo("val");
        assertThat(row.getList("liststring", String.class)).containsExactly("one");
        assertThat(row.getSet("setstring", String.class)).containsExactly("two");
        assertThat(row.getMap("mapstring", Integer.class, String.class)).hasSize(1).containsEntry(3, "three");
    }
}
