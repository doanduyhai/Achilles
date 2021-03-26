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
import static info.archinnov.achilles.generated.function.SystemFunctions.token;
import static info.archinnov.achilles.generated.meta.entity.EntityWithCompositePartitionKey_AchillesMeta.COLUMNS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithCompositePartitionKey_Manager;
import info.archinnov.achilles.internals.entities.EntityWithCompositePartitionKey;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;

public class TestEntityWithCompositePartitionKey {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithCompositePartitionKey.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithCompositePartitionKey.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithCompositePartitionKey_Manager manager = resource.getManagerFactory().forEntityWithCompositePartitionKey();

    @Test
    public void should_insert() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = new UUID(1L, 1L);
        final EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, uuid, "val");

        //When
        manager.crud().insert(entity).execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM entity_composite_pk WHERE id = "
                + id + " AND uuid = " + uuid).all();
        assertThat(rows).hasSize(1);

        final Row row = rows.get(0);
        assertThat(row.getLong("id")).isEqualTo(id);
        assertThat(row.getUUID("uuid")).isEqualTo(uuid);
        assertThat(row.getString("value")).isEqualTo("val");
    }

    @Test
    public void should_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = new UUID(1L, 1L);
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql", ImmutableMap.of("id", id, "uuid", uuid, "value", "val"));

        //When
        final EntityWithCompositePartitionKey actual = manager
                .crud()
                .findById(id, uuid)
                .get();

        //Then

        assertThat(actual.getValue()).isEqualTo("val");
    }

    @Test
    public void should_dsl_select_with_token_value() throws Exception {
        //Given
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id3 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id4 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id5 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid1 = new UUID(1L, 1L);
        final UUID uuid2 = new UUID(2L, 2L);
        final UUID uuid3 = new UUID(3L, 3L);
        final UUID uuid4 = new UUID(4L, 4L);
        final UUID uuid5 = new UUID(5L, 5L);

        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id1, "uuid", uuid1, "value", "val1"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id2, "uuid", uuid2, "value", "val2"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id3, "uuid", uuid3, "value", "val3"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id4, "uuid", uuid4, "value", "val4"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id5, "uuid", uuid5, "value", "val5"));

        final List<Tuple2<Long, UUID>> tuple2s = session.execute("SELECT token(id, uuid) AS tokens,uuid FROM achilles_embedded.entity_composite_pk LIMIT 6")
                .all()
                .stream()
                .map(row -> Tuple2.of(row.getLong("tokens"), row.getUUID("uuid")))
                .sorted(Comparator.comparing(Tuple2::_1))
                .limit(2)
                .collect(toList());

        final Long token = tuple2s.get(1)._1();

        //When
        final List<EntityWithCompositePartitionKey> actuals = manager
                .dsl()
                .select()
                .uuid()
                .fromBaseTable()
                .where()
                .tokenValueOf_id_uuid().Lte(token)
                .getList();

        //Then
        assertThat(actuals).hasSize(2);
        assertThat(actuals
                .stream()
                .map(EntityWithCompositePartitionKey::getUuid)
                .sorted()
                .collect(toList()))
                .containsOnlyElementsOf(tuple2s.stream().map(Tuple2::_2).collect(toSet()));
    }

    @Test
    public void should_dsl_select_with_IN_clause() throws Exception {
        //Given
        final long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid1 = new UUID(1L, 1L);
        final UUID uuid2 = new UUID(2L, 2L);
        final UUID uuid3 = new UUID(3L, 3L);

        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id1, "uuid", uuid1, "value", "val1-1"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id1, "uuid", uuid2, "value", "val1-2"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id2, "uuid", uuid1, "value", "val2-1"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id2, "uuid", uuid2, "value", "val2-2"));
        scriptExecutor.executeScriptTemplate("EntityWithCompositePartitionKey/insert_single_row.cql",
                ImmutableMap.of("id", id2, "uuid", uuid3, "value", "val2-3"));


        //When
        final List<EntityWithCompositePartitionKey> actuals = manager
                .dsl()
                .select()
                .value()
                .fromBaseTable()
                .where()
                .id().IN(id1, id2)
                .uuid().IN(uuid1, uuid3)
                .getList();

        //Then
        assertThat(actuals).hasSize(3);
        assertThat(actuals
                .stream()
                .map(EntityWithCompositePartitionKey::getValue)
                .sorted()
                .collect(toList()))
                .containsExactly("val1-1", "val2-1", "val2-3");
    }

    @Test
    public void should_dsl_select_using_token_function() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = new UUID(1L, 1L);
        final EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, uuid, "val");
        manager.crud().insert(entity).execute();


        //When
        TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .uuid()
                .function(token(COLUMNS.PARTITION_KEYS), "tokens")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("tokens")).isNotNull();

    }
}
