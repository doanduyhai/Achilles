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

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityAsChild_Manager;
import info.archinnov.achilles.internals.dsl.crud.DeleteWithOptions;
import info.archinnov.achilles.internals.dsl.crud.InsertWithOptions;
import info.archinnov.achilles.internals.entities.EntityAsChild;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestALLEntityAsChild {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityAsChild.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityAsChild.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityAsChild_Manager manager = resource.getManagerFactory().forEntityAsChild();

    @Test
    public void should_insert() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityAsChild entity = new EntityAsChild(id, "val", "child_val");

        //When
        manager.crud().insert(entity).execute();

        //Then
        final Row row = session.execute("SELECT * FROM entity_child").one();
        assertThat(row.getLong("id")).isEqualTo(id);
        assertThat(row.getString("value")).isEqualTo("val");
        assertThat(row.getString("child_value")).isEqualTo("child_val");
    }

    @Test
    public void should_insert_generate_query_and_bound_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityAsChild entity = new EntityAsChild(id, "val", "child_val");

        //When
        final InsertWithOptions<EntityAsChild> insert = manager
                .crud()
                .insert(entity)
                .usingTimeToLive(123)
                .usingTimestamp(100L);

        //Then

        String expectedQuery = "INSERT INTO " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME
                + ".entity_child (id,child_value,value) " +
                "VALUES (:id,:child_value,:value) " +
                "USING TTL :ttl;";

        assertThat(insert.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(insert.getBoundValues()).containsExactly(id, "child_val", "val", 123);
        assertThat(insert.getEncodedBoundValues()).containsExactly(id, "child_val", "val", 123);
        assertThat(insert.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_find_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityAsChild/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final EntityAsChild actual = manager.crud().findById(id).get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getValue()).isEqualTo("val");
        assertThat(actual.getAnotherValue()).isEqualTo("child_val");
    }

    @Test
    public void should_delete_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityAsChild/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager.crud().deleteById(id).execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM entity_child WHERE id = " + id).all();
        assertThat(rows).isEmpty();
    }

    @Test
    public void should_delete_entity_generate_query_and_bound_values() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityAsChild entity = new EntityAsChild(id, "val", "child_val");

        //When
        final DeleteWithOptions<EntityAsChild> delete = manager
                .crud()
                .delete(entity);

        //Then

        String expectedQuery = "DELETE FROM " + DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME + ".entity_child " +
                "WHERE id=:id;";

        assertThat(delete.getStatementAsString()).isEqualTo(expectedQuery);
        assertThat(delete.getBoundValues()).containsExactly(id);
        assertThat(delete.getEncodedBoundValues()).containsExactly(id);
        assertThat(delete.generateAndGetBoundStatement().preparedStatement().getQueryString()).isEqualTo(expectedQuery);
    }

    @Test
    public void should_dsl_select_one() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityAsChild/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        final EntityAsChild actual = manager
                .dsl()
                .select()
                .value()
                .anotherValue()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getValue()).isEqualTo("val");
        assertThat(actual.getAnotherValue()).isEqualTo("child_val");
    }

    @Test
    public void should_dsl_delete() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityAsChild/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .delete()
                .anotherValue()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row row = session.execute("SELECT * FROM entity_child WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.isNull("child_value")).isTrue();
        assertThat(row.getString("value")).isEqualTo("val");
    }

    @Test
    public void should_dsl_update_child_value() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityAsChild/insert_single_row.cql", ImmutableMap.of("id", id, "table", "simple"));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .anotherValue().Set("another_child_val")
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row row = session.execute("SELECT child_value FROM entity_child WHERE id = " + id).one();
        assertThat(row).isNotNull();
        assertThat(row.getString("child_value")).isEqualTo("another_child_val");
    }
}
