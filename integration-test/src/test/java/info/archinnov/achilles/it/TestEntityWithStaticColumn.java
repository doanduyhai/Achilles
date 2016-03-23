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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithStaticColumn_Manager;
import info.archinnov.achilles.internals.entities.EntityWithStaticColumn;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

@RunWith(MockitoJUnitRunner.class)
public class TestEntityWithStaticColumn {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithStaticColumn.class)
            .truncateBeforeAndAfterTest()
            .withScript("functions/createFunctions.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithStaticColumn.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithStaticColumn_Manager manager = resource.getManagerFactory().forEntityWithStaticColumn();

    @Test
    public void should_insert() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();

        final EntityWithStaticColumn entity = new EntityWithStaticColumn(id, uuid, "static_val", "val");

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entitywithstaticcolumn WHERE id = " + id + " AND uuid = " + uuid).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("static_col")).isEqualTo("static_val");
        assertThat(actual.getString("value")).isEqualTo("val");
    }

    @Test
    public void should_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();

        scriptExecutor.executeScriptTemplate("EntityWithStaticColumn/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        final EntityWithStaticColumn actual = manager
                .crud()
                .findById(id, uuid)
                .get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getStaticCol()).isEqualTo("static_val");
        assertThat(actual.getValue()).isEqualTo("val");
    }

    @Test
    public void should_dsl_select_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();

        scriptExecutor.executeScriptTemplate("EntityWithStaticColumn/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        final EntityWithStaticColumn actual = manager
                .dsl()
                .select()
                .value()
                .staticCol()
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getStaticCol()).isEqualTo("static_val");
        assertThat(actual.getValue()).isEqualTo("val");
    }

    @Test
    public void should_dsl_update_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();

        scriptExecutor.executeScriptTemplate("EntityWithStaticColumn/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        manager
                .dsl()
                .updateStatic()
                .fromBaseTable()
                .staticCol_Set("updated_static")
                .where()
                .id_Eq(id)
                .ifStaticCol_Eq("static_val")
                .execute();

        //Then
        final Row actual = session.execute("SELECT static_col FROM entitywithstaticcolumn WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("static_col")).isEqualTo("updated_static");
    }

    @Test
    public void should_dsl_delete_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();

        scriptExecutor.executeScriptTemplate("EntityWithStaticColumn/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        manager
                .dsl()
                .deleteStatic()
                .staticCol()
                .fromBaseTable()
                .where()
                .id_Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT static_col FROM entitywithstaticcolumn WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.isNull("static_col")).isTrue();
    }
}
