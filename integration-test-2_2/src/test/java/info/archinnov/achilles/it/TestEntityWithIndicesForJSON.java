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

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_2_2;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_2_2;
import info.archinnov.achilles.generated.manager.EntityWithIndicesForJSON_Manager;
import info.archinnov.achilles.internals.entities.EntityWithIndicesForJSON;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithIndicesForJSON {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_2_2> resource = AchillesTestResourceBuilder
            .forJunit()
            .withScript("functions/createFunctions.cql")
            .entityClassesToTruncate(EntityWithIndicesForJSON.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_2_2
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithIndicesForJSON.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithIndicesForJSON_Manager manager = resource.getManagerFactory().forEntityWithIndicesForJSON();

    @Test
    public void should_query_using_simple_index_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq_FromJson("\"313\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getFullIndexOnCollection()).containsExactly("313");
    }

    @Test
    public void should_query_using_collection_index_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_collectionIndex().Contains_FromJson("\"4\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("411");
    }

    @Test
    public void should_query_using_full_collection_index_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_fullIndexOnCollection().Eq_FromJson("[\"311\"]")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("311");
    }

    @Test
    public void should_query_using_map_key_index_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_indexOnMapKey().ContainsKey_FromJSON("\"312\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("312");
    }

    @Test
    public void should_query_using_map_key_value_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_indexOnMapValue().ContainsValue_FromJSON("\"211\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("211");
    }

    @Test
    public void should_query_using_map_key_entry_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_indexOnMapEntry().ContainsEntry_FromJSON("212", "\"212\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("212");
    }

    @Test
    public void should_query_using_index_and_partition_key_asJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<String> actual = manager
                .indexed()
                .select()
                .allColumnsAsJSON_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("313")
                .id().Eq(id)
                .getListJSON();

        //Then
        assertThat(actual).hasSize(1);
        final String json = actual.get(0);
        assertThat(json).isEqualTo("{\"id\": " + id + ", \"clust1\": 3, \"clust2\": 1, \"clust3\": \"3\", \"collectionindex\": [\"3\", \"1\", \"3\"], \"fullindexoncollection\": [\"313\"], \"indexonmapentry\": {\"313\": \"313\"}, \"indexonmapkey\": {\"313\": \"313\"}, \"indexonmapvalue\": {\"313\": \"313\"}, \"simpleindex\": \"313\"}");
    }

    @Test
    public void should_query_using_index_and_clustering_column_fromJSON() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithIndicesForJSON/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithIndicesForJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_simpleIndex().Eq("312")
                .clust1().Eq_FromJson("3")
                .clust3().Eq_FromJson("\"2\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        final EntityWithIndicesForJSON entity = actual.get(0);
        assertThat(entity.getSimpleIndex()).isEqualTo("312");
    }

}
