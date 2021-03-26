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

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithCounterColumn_Manager;
import info.archinnov.achilles.internals.entities.EntityWithCounterColumn;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithCounterColumn {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithCounterColumn.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithCounterColumn.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithCounterColumn_Manager manager = resource.getManagerFactory().forEntityWithCounterColumn();

    @Test
    public void should_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long incr = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithCounterColumn/insert_single_row.cql", ImmutableMap.of("id", id, "incr", incr));

        //When
        final EntityWithCounterColumn actual = manager
                .crud()
                .findById(id)
                .get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.getCount()).isEqualTo(incr);
    }

    @Test
    public void should_dsl_update() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long incr = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .count().Incr(incr)
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT count FROM entity_counter WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getLong("count")).isEqualTo(incr);
    }

    @Test
    public void should_delete_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long incr = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithCounterColumn/insert_single_row.cql", ImmutableMap.of("id", id, "incr", incr));

        //When
        manager
                .crud()
                .deleteById(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entity_counter WHERE id = " + id).one();
        assertThat(actual).isNull();
    }

    @Test
    public void should_delete_instance() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long incr = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithCounterColumn entity = new EntityWithCounterColumn(id, incr);

        //When
        manager
                .crud()
                .delete(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entity_counter WHERE id = " + id).one();
        assertThat(actual).isNull();
    }

    @Test
    public void should_dsl_delete() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final long incr = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithCounterColumn/insert_single_row.cql", ImmutableMap.of("id", id, "incr", incr));

        //When
        manager
                .dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM entity_counter WHERE id = " + id).one();
        assertThat(actual).isNull();
    }
}
