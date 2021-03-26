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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

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
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener.LWTResult;
import info.archinnov.achilles.type.strategy.InsertStrategy;

public class TestEntityWithStaticColumn {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithStaticColumn.class)
            .truncateBeforeAndAfterTest()
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
    public void should_update() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        scriptExecutor.executeScriptTemplate("EntityWithStaticColumn/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));
        final EntityWithStaticColumn entity = new EntityWithStaticColumn(id, uuid, "new_static", "new_val");

        //When
        manager
                .crud()
                .update(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entitywithstaticcolumn WHERE id = " + id + " AND uuid = " + uuid).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("static_col")).isEqualTo("new_static");
        assertThat(actual.getString("value")).isEqualTo("new_val");
    }

    @Test
    public void should_insert_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();

        scriptExecutor.executeScriptTemplate("EntityWithStaticColumn/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid));
        final EntityWithStaticColumn entity = new EntityWithStaticColumn(id, null, "new_static", "new_val");

        //When
        manager
                .crud()
                .updateStatic(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entitywithstaticcolumn WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("static_col")).isEqualTo("new_static");
        assertThat(actual.getString("value")).isEqualTo("val");
    }

    @Test
    public void should_update_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        final EntityWithStaticColumn entity = new EntityWithStaticColumn(id, null, "static_val", "val");

        //When
        manager
                .crud()
                .insertStatic(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entitywithstaticcolumn WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("static_col")).isEqualTo("static_val");
        assertThat(actual.isNull("value")).isTrue();
    }

    @Test
    public void should_insert_static_with_insert_strategy() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        final EntityWithStaticColumn entity1 = new EntityWithStaticColumn(id, null, "static_val1", "another_static_val1", null);
        final EntityWithStaticColumn entity2 = new EntityWithStaticColumn(id, null, null, "another_static_val2", null);
        manager
                .crud()
                .insertStatic(entity1)
                .execute();

        //When
        manager
                .crud()
                .insertStatic(entity2)
                .withInsertStrategy(InsertStrategy.NOT_NULL_FIELDS)
                .execute();
        //Then
        final Row actual = session.execute("SELECT * FROM entitywithstaticcolumn WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("static_col")).isEqualTo("static_val1");
        assertThat(actual.getString("another_static_col")).isEqualTo("another_static_val2");
    }

    @Test
    public void should_insert_static_if_not_exist() throws Exception {
        //Given
        final AtomicReference<LWTResult> lwtResultRef = new AtomicReference<>();
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticColumn entity = new EntityWithStaticColumn(id, null, "static_val", null);

        manager
                .crud()
                .insertStatic(entity)
                .ifNotExists()
                .execute();

        //When
        manager
                .crud()
                .insertStatic(entity)
                .ifNotExists()
                .withLwtResultListener(lwtResultRef::set)
                .execute();

        //Then
        assertThat(lwtResultRef.get()).isNotNull();
        assertThat(lwtResultRef.get().currentValues().<String>getTyped("static_col")).isEqualTo("static_val");
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
                .id().Eq(id)
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
                .staticCol().Set("updated_static")
                .where()
                .id().Eq(id)
                .if_StaticCol().Eq("static_val")
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
                .id().Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT static_col FROM entitywithstaticcolumn WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.isNull("static_col")).isTrue();
    }
}
