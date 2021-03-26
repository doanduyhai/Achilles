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
import info.archinnov.achilles.generated.manager.EntityWithComplexCounters_Manager;
import info.archinnov.achilles.internals.entities.EntityWithComplexCounters;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithComplexCounter {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithComplexCounters.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithComplexCounters.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithComplexCounters_Manager manager = resource.getManagerFactory().forEntityWithComplexCounters();

    @Test
    public void should_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final long count = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long staticCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long codecCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithComplexCounters/insert_single_row.cql",
                ImmutableMap.of("id", id, "uuid", uuid, "count", count, "staticCount", staticCount, "codecCount", codecCount));

        //When
        final EntityWithComplexCounters actual = manager
                .crud()
                .findById(id, uuid)
                .get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getSimpleCounter()).isEqualTo(count);
        assertThat(actual.getStaticCounter()).isEqualTo(staticCount);
        assertThat(actual.getCounterWithCodec()).isEqualTo(codecCount + "");
    }

    @Test
    public void should_dsl_update_codec_counter() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = UUIDs.timeBased();
        final long codecCount = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .counterWithCodec().Incr(codecCount + "")
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .execute();

        //Then
        final Row actual = session.execute("SELECT codec_count FROM entity_complex_counters WHERE id = " + id + " AND uuid = " + uuid).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getLong("codec_count")).isEqualTo(codecCount);
    }
}
