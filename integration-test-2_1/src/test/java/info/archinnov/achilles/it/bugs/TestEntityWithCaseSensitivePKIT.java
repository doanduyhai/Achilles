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

package info.archinnov.achilles.it.bugs;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithCaseSensitivePK_Manager;
import info.archinnov.achilles.internals.entities.EntityWithCaseSensitivePK;
import info.archinnov.achilles.internals.entities.UDTWithNoKeyspace;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithCaseSensitivePKIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithCaseSensitivePK.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithCaseSensitivePK.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private EntityWithCaseSensitivePK_Manager manager = resource.getManagerFactory().forEntityWithCaseSensitivePK();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    @Test
    public void should_insert_and_find() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Long clust = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Integer priority = 1;
        final EntityWithCaseSensitivePK entity = new EntityWithCaseSensitivePK(id, clust, priority);
        entity.setList(Arrays.asList("1", "2"));
        entity.setSet(Sets.newHashSet("1", "2"));
        entity.setMap(ImmutableMap.of(1, "1", 2, "2"));
        entity.setUdt(new UDTWithNoKeyspace(id, "test"));

        //When
        manager.crud().insert(entity).execute();

        //Then
        final EntityWithCaseSensitivePK found = manager.crud().findById(id, clust, priority).get();
        assertThat(found).isNotNull();
        assertThat(found.getList()).containsExactly("1", "2");
        assertThat(found.getSet()).containsExactly("1", "2");
        assertThat(found.getMap()).containsEntry(1, "1");
        assertThat(found.getMap()).containsEntry(2, "2");
        assertThat(found.getUdt().getId()).isEqualTo(id);
        assertThat(found.getUdt().getValue()).isEqualTo("test");

    }

    @Test
    public void should_insert_and_delete() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Long clust = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Integer priority = 1;

        //When
        manager.crud().insert(new EntityWithCaseSensitivePK(id, clust, priority)).execute();
        manager.crud().deleteById(id, clust, priority).execute();

        //Then
        final EntityWithCaseSensitivePK found = manager.crud().findById(id, clust, priority).get();
        assertThat(found).isNull();
    }

    @Test
    public void should_dsl_select() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithCaseSensitivePK/insert1row.cql", ImmutableMap.of("partitionKey",id));

        //When
        final EntityWithCaseSensitivePK found = manager
                .dsl()
                .select()
                .list()
                .set()
                .map()
                .udt().allColumns()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getOne();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getList()).containsExactly("1", "2");
        assertThat(found.getSet()).containsExactly("1", "2");
        assertThat(found.getMap()).containsEntry(1, "1");
        assertThat(found.getMap()).containsEntry(2, "2");
        assertThat(found.getUdt().getId()).isEqualTo(1L);
        assertThat(found.getUdt().getValue()).isEqualTo("test");
    }

    @Test
    public void should_dsl_select_with_tuple_relations() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithCaseSensitivePK/insert1row.cql", ImmutableMap.of("partitionKey",id));

        //When
        final EntityWithCaseSensitivePK found = manager
                .dsl()
                .select()
                .list()
                .set()
                .map()
                .udt().allColumns()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .clust_priority().Gte_And_Lte(9L, 1, 11L, 1)
                .getOne();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getList()).containsExactly("1", "2");
        assertThat(found.getSet()).containsExactly("1", "2");
        assertThat(found.getMap()).containsEntry(1, "1");
        assertThat(found.getMap()).containsEntry(2, "2");
        assertThat(found.getUdt().getId()).isEqualTo(1L);
        assertThat(found.getUdt().getValue()).isEqualTo("test");
    }

    @Test
    public void should_dsl_select_with_token_value() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithCaseSensitivePK/insert1row.cql", ImmutableMap.of("partitionKey",id));

        //When
        final EntityWithCaseSensitivePK found = manager
                .dsl()
                .select()
                .list()
                .set()
                .map()
                .udt().allColumns()
                .fromBaseTable()
                .where()
                .tokenValueOf_partitionKey().Gt(Long.MIN_VALUE)
                .getOne();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getList()).containsExactly("1", "2");
        assertThat(found.getSet()).containsExactly("1", "2");
        assertThat(found.getMap()).containsEntry(1, "1");
        assertThat(found.getMap()).containsEntry(2, "2");
        assertThat(found.getUdt().getId()).isEqualTo(1L);
        assertThat(found.getUdt().getValue()).isEqualTo("test");
    }

    @Test
    public void should_dsl_update() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithCaseSensitivePK/insert1row.cql", ImmutableMap.of("partitionKey",id));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .list().AppendTo("3")
                .map().PutTo(3, "3")
                .where()
                .id().Eq(id)
                .clust().Eq(10L)
                .priority().Eq(1)
                .execute();

        //Then
        final EntityWithCaseSensitivePK found = manager.crud().findById(id, 10L, 1).get();
        assertThat(found).isNotNull();
        assertThat(found.getList()).containsExactly("1", "2", "3");
        assertThat(found.getSet()).containsExactly("1", "2");
        assertThat(found.getMap()).containsEntry(1, "1");
        assertThat(found.getMap()).containsEntry(2, "2");
        assertThat(found.getMap()).containsEntry(3, "3");
        assertThat(found.getUdt().getId()).isEqualTo(1L);
        assertThat(found.getUdt().getValue()).isEqualTo("test");
    }

    @Test
    public void should_dsl_delete() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithCaseSensitivePK/insert1row.cql", ImmutableMap.of("partitionKey", id));

        //When
        manager
                .dsl()
                .delete()
                .list()
                .map()
                .set()
                .udt()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .clust().Eq(10L)
                .priority().Eq(1)
                .execute();

        //Then
        final EntityWithCaseSensitivePK found = manager.crud().findById(id, 10L, 1).get();
        assertThat(found).isNotNull();
        assertThat(found.getList()).isNull();
        assertThat(found.getSet()).isNull();
        assertThat(found.getMap()).isNull();
        assertThat(found.getUdt()).isNull();
    }
}
