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

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithStaticCounterColumn_Manager;
import info.archinnov.achilles.internals.entities.EntityWithStaticCounterColumn;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithStaticCounterColumn {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithStaticCounterColumn.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithStaticCounterColumn.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithStaticCounterColumn_Manager manager = resource.getManagerFactory().forEntityWithStaticCounterColumn();

    @Test
    public void should_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final long count = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long staticCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithStaticCounterColumn/insert_single_row.cql",
                ImmutableMap.of("id", id, "uuid", uuid, "count", count, "staticCount", staticCount));

        //When
        final EntityWithStaticCounterColumn actual = manager
                .crud()
                .findById(id, uuid)
                .get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getCount()).isEqualTo(count);
        assertThat(actual.getStaticCount()).isEqualTo(staticCount);
    }

    @Test
    public void should_dsl_update_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long staticCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        //When
        manager
                .dsl()
                .updateStatic()
                .fromBaseTable()
                .staticCount().Incr(staticCount)
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT static_count FROM entity_static_counter WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getLong("static_count")).isEqualTo(staticCount);
    }

    @Test
    public void should_delete_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final long count = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long staticCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithStaticCounterColumn/insert_single_row.cql",
                ImmutableMap.of("id", id, "uuid", uuid, "count", count, "staticCount", staticCount));

        //When
        manager
                .crud()
                .deleteByPartitionKeys(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entity_static_counter WHERE id = " + id).one();
        assertThat(actual).isNull();
    }

    @Test
    public void should_delete_instance() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final EntityWithStaticCounterColumn entity = new EntityWithStaticCounterColumn(id, uuid, null, null);

        //When
        manager
                .crud()
                .delete(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entity_static_counter WHERE id = " + id).one();
        assertThat(actual).isNull();
    }

    @Test
    public void should_dsl_delete_static() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final long count = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long staticCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithStaticCounterColumn/insert_single_row.cql",
                ImmutableMap.of("id", id, "uuid", uuid, "count", count, "staticCount", staticCount));

        //When
        manager
                .dsl()
                .deleteStatic()
                .staticCount()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entity_static_counter WHERE id = " + id).one();
        assertThat(actual).isNotNull();
        assertThat(actual.getLong("count")).isEqualTo(count);
        assertThat(actual.isNull("static_count")).isTrue();
    }
}
