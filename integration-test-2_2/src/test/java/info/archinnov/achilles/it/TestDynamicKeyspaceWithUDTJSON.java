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

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_2_2;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_2_2;
import info.archinnov.achilles.generated.manager.EntityWithNoKeyspaceUDT_Manager;
import info.archinnov.achilles.internals.entities.EntityWithNoKeyspaceUDT;
import info.archinnov.achilles.internals.entities.NoKeyspaceUDT;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.SchemaNameProvider;

public class TestDynamicKeyspaceWithUDTJSON {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_2_2> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithNoKeyspaceUDT.class)
            .truncateBeforeAndAfterTest()
            .withScript("functions/createFunctions.cql")
            .withScript("EntityWithNoKeyspaceUDT/create_alternate_schema.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_2_2
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithNoKeyspaceUDT.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private EntityWithNoKeyspaceUDT_Manager manager = resource.getManagerFactory().forEntityWithNoKeyspaceUDT();
    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    private SchemaNameProvider provider = new SchemaNameProvider() {
        @Override
        public <T> String keyspaceFor(Class<T> entityClass) {
            return "dynamic_ks_json";
        }

        @Override
        public <T> String tableNameFor(Class<T> entityClass) {
            return "dynamic_table_json";
        }
    };

    @Test
    public void should_crud_insert_json_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        //When
        manager
                .crud()
                .withSchemaNameProvider(provider)
                .insertJSON(format("{\"id\": %s, \"clust\": {\"id\": %s, \"value\" : \"value\"}, \"udt\": {\"id\": %s, \"value\" : \"value\"}}", id, id, id))
                .execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM dynamic_ks_json.dynamic_table_json WHERE id = " + id).all();
        assertThat(rows).hasSize(1);
    }

    @Test
    public void should_crud_select_json_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final NoKeyspaceUDT udt = new NoKeyspaceUDT(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithNoKeyspaceUDT/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final String json = manager
                .dsl()
                .select()
                .allColumnsAsJSON_From(provider)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .getJSON();

        //Then
        assertThat(json).isEqualTo(format("{\"id\": %s, \"clust\": {\"id\": %s, \"value\": \"value\"}, \"udt\": {\"id\": %s, \"value\": \"value\"}}", id, id, id));
    }

    @Test
    public void should_crud_delete_json_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final NoKeyspaceUDT udt = new NoKeyspaceUDT(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithNoKeyspaceUDT/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .allColumns_From(provider)
                .where()
                .id().Eq(id)
                .clust().Eq_FromJson(format("{\"id\": %s, \"value\": \"value\"}", id))
                .execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM dynamic_ks_json.dynamic_table_json WHERE id = " + id).all();
        assertThat(rows).hasSize(0);
    }
}
