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

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithComplexIndices_Manager;
import info.archinnov.achilles.internals.entities.EntityWithComplexIndices;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithComplexIndices {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithComplexIndices.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithComplexIndices.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithComplexIndices_Manager manager = resource.getManagerFactory().forEntityWithComplexIndices();

    @Test
    public void should_query_using_simple_index() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("313")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getFullIndexOnCollection()).containsExactly("313");
    }

    @Test
    public void should_query_using_collection_index() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_collectionIndex().Contains("4")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("411");
    }

    @Test
    public void should_query_using_full_collection_index() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_fullIndexOnCollection().Eq(Sets.newHashSet("311"))
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("311");
    }

    @Test
    public void should_query_using_map_key_index() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_indexOnMapKey().ContainsKey("312")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("312");
    }

    @Test
    public void should_query_using_map_key_value() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_indexOnMapValue().ContainsValue("211")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("211");
    }

    @Test
    public void should_query_using_map_key_entry() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_indexOnMapEntry().ContainsEntry(212, "212")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("212");
    }

    @Test
    public void should_query_using_index_and_partition_key() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("313")
                .id().Eq(id)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("313");
    }

    @Test
    public void should_query_using_index_and_clustering_column() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("312")
                .clust1().Eq(3)
                .clust3().Eq("2")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("312");
    }

    @Test
    public void should_query_using_index_and_clustering_column_slice() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("312")
                .clust1().Gte_And_Lte(1, 4)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("312");
    }

    @Test
    public void should_query_using_index_and_clustering_column_inequality() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("312")
                .clust1().Gte(5)
                .getList();

        //Then
        assertThat(actual).hasSize(0);
    }

    @Test
    public void should_query_using_index_and_multi_clustering_columns_slice() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithComplexIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("312")
                .clust1_clust2_clust3().clust1_And_clust2_And_clust3_Gte_And_clust1_And_clust2_Lte(1, 1, "1", 3, 2)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithComplexIndices entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("312");
    }
}
