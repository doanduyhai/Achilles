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

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

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

@RunWith(MockitoJUnitRunner.class)
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
                .collect(Collectors.toList()))
                .containsExactly("val1-1", "val2-1", "val2-3");
    }
}
