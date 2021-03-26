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

import static info.archinnov.achilles.generated.meta.entity.EntityForGroupBy_AchillesMeta.COLUMNS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_10;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_10;
import info.archinnov.achilles.generated.function.SystemFunctions;
import info.archinnov.achilles.generated.manager.EntityForGroupBy_Manager;
import info.archinnov.achilles.generated.meta.entity.EntityForGroupBy_AchillesMeta;
import info.archinnov.achilles.internals.entities.EntityForGroupBy;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;

public class TestEntityForGroupByIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_10> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_10")
            .entityClassesToTruncate(EntityForGroupBy.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_10
                    .builder(cluster)
                    .withDefaultKeyspaceName("it_3_10")
                    .withManagedEntityClasses(EntityForGroupBy.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityForGroupBy_Manager manager = resource.getManagerFactory().forEntityForGroupBy();

    @Test
    public void should_select_sum_with_group_by_first_clustering() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = new UUID(1L,0L);
        scriptExecutor.executeScriptTemplate("EntityForGroupBy/insert_rows.cql", ImmutableMap.of("id", id, "uuid", uuid));
        final EntityForGroupBy_AchillesMeta.ColumnsForFunctions columns = COLUMNS;

        //When
        final List<TypedMap> typedMaps = manager
                .dsl()
                .select()
                .clust1()
                .function(SystemFunctions.sum(COLUMNS.VAL), "sum_val")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .groupBy()
                .clust1()
                .orderByClust1Ascending()
                .getTypedMaps();

        //Then
        assertThat(typedMaps).hasSize(3);

        final TypedMap clustering_1 = typedMaps.get(0);
        assertThat(clustering_1.<Integer>getTyped("clust1")).isEqualTo(1);
        assertThat(clustering_1.<Integer>getTyped("sum_val")).isEqualTo(9);

        final TypedMap clustering_2 = typedMaps.get(1);
        assertThat(clustering_2.<Integer>getTyped("clust1")).isEqualTo(2);
        assertThat(clustering_2.<Integer>getTyped("sum_val")).isEqualTo(6);

        final TypedMap clustering_3 = typedMaps.get(2);
        assertThat(clustering_3.<Integer>getTyped("clust1")).isEqualTo(3);
        assertThat(clustering_3.<Integer>getTyped("sum_val")).isEqualTo(3);
    }

    @Test
    public void should_select_sum_with_group_by_first_and_second_clustering() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = new UUID(1L,0L);
        scriptExecutor.executeScriptTemplate("EntityForGroupBy/insert_rows.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        final List<TypedMap> typedMaps = manager
                .dsl()
                .select()
                .clust1()
                .clust2()
                .function(SystemFunctions.sum(COLUMNS.VAL), "sum_val")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .groupBy()
                .clust1_clust2()
                .orderByClust1Ascending()
                .getTypedMaps();

        //Then
        assertThat(typedMaps).hasSize(6);

        final TypedMap row1 = typedMaps.get(0);
        assertThat(row1.<Integer>getTyped("clust1")).isEqualTo(1);
        assertThat(row1.<Integer>getTyped("clusteRing2")).isEqualTo(1);
        assertThat(row1.<Integer>getTyped("sum_val")).isEqualTo(3);

        final TypedMap row2 = typedMaps.get(1);
        assertThat(row2.<Integer>getTyped("clust1")).isEqualTo(1);
        assertThat(row2.<Integer>getTyped("clusteRing2")).isEqualTo(2);
        assertThat(row2.<Integer>getTyped("sum_val")).isEqualTo(3);

        final TypedMap row3 = typedMaps.get(2);
        assertThat(row3.<Integer>getTyped("clust1")).isEqualTo(1);
        assertThat(row3.<Integer>getTyped("clusteRing2")).isEqualTo(3);
        assertThat(row3.<Integer>getTyped("sum_val")).isEqualTo(3);

        final TypedMap row4 = typedMaps.get(3);
        assertThat(row4.<Integer>getTyped("clust1")).isEqualTo(2);
        assertThat(row4.<Integer>getTyped("clusteRing2")).isEqualTo(1);
        assertThat(row4.<Integer>getTyped("sum_val")).isEqualTo(3);

        final TypedMap row5 = typedMaps.get(4);
        assertThat(row5.<Integer>getTyped("clust1")).isEqualTo(2);
        assertThat(row5.<Integer>getTyped("clusteRing2")).isEqualTo(2);
        assertThat(row5.<Integer>getTyped("sum_val")).isEqualTo(3);

        final TypedMap row6 = typedMaps.get(5);
        assertThat(row6.<Integer>getTyped("clust1")).isEqualTo(3);
        assertThat(row6.<Integer>getTyped("clusteRing2")).isEqualTo(1);
        assertThat(row6.<Integer>getTyped("sum_val")).isEqualTo(3);
    }

    @Test
    public void should_select_sum_with_group_by_second_clustering() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UUID uuid = new UUID(1L,0L);
        scriptExecutor.executeScriptTemplate("EntityForGroupBy/insert_rows.cql", ImmutableMap.of("id", id, "uuid", uuid));

        //When
        final List<TypedMap> typedMaps = manager
                .dsl()
                .select()
                .clust1()
                .clust2()
                .function(SystemFunctions.sum(COLUMNS.VAL), "sum_val")
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .uuid().Eq(uuid)
                .clust1().Eq(2)
                .groupBy()
                .clust2()
                .getTypedMaps();

        //Then
        assertThat(typedMaps).hasSize(2);

        final TypedMap row1 = typedMaps.get(0);
        assertThat(row1.<Integer>getTyped("clust1")).isEqualTo(2);
        assertThat(row1.<Integer>getTyped("clusteRing2")).isEqualTo(1);
        assertThat(row1.<Integer>getTyped("sum_val")).isEqualTo(3);

        final TypedMap row2 = typedMaps.get(1);
        assertThat(row2.<Integer>getTyped("clust1")).isEqualTo(2);
        assertThat(row2.<Integer>getTyped("clusteRing2")).isEqualTo(2);
        assertThat(row2.<Integer>getTyped("sum_val")).isEqualTo(3);
    }

    @Test
    public void should_select_sum_group_by_partition_keys() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForGroupBy/insert_multi_partitions.cql", ImmutableMap.of("id", id));

        //When
        final List<TypedMap> typedMaps = manager
                .dsl()
                .select()
                .id()
                .uuid()
                .function(SystemFunctions.sum(COLUMNS.VAL), "sum_val")
                .fromBaseTable()
                .groupBy()
                .id_uuid()
                .getTypedMaps();

        //Then
        assertThat(typedMaps).hasSize(2);

        final TypedMap row1 = typedMaps.get(0);
        if (row1.<UUID>getTyped("uuID").equals(new UUID(1L, 0L))) {
            assertThat(row1.<Integer>getTyped("sum_val")).isEqualTo(6);
        } else {
            assertThat(row1.<Integer>getTyped("sum_val")).isEqualTo(5);
        }

        final TypedMap row2 = typedMaps.get(1);
        if (row2.<UUID>getTyped("uuID").equals(new UUID(1L, 0L))) {
            assertThat(row2.<Integer>getTyped("sum_val")).isEqualTo(6);
        } else {
            assertThat(row2.<Integer>getTyped("sum_val")).isEqualTo(5);
        }
    }

    @Test
    public void should_select_sum_group_by_partition_keys_and_one_clustering() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForGroupBy/insert_multi_partitions.cql", ImmutableMap.of("id", id));


        //When
        final List<TypedMap> typedMaps = manager
                .dsl()
                .select()
                .id()
                .uuid()
                .clust1()
                .function(SystemFunctions.sum(COLUMNS.VAL), "sum_val")
                .fromBaseTable()
                .groupBy()
                .id_uuid_clust1()
                .getTypedMaps();

        //Then
        assertThat(typedMaps).hasSize(6);

        final TypedMap row1 = typedMaps.get(0);
        if (row1.<UUID>getTyped("uuID").equals(new UUID(1L, 0L))) {

            assertThat(row1.<Integer>getTyped("clust1")).isEqualTo(1);
            assertThat(row1.<Integer>getTyped("sum_val")).isEqualTo(3);

            final TypedMap row2 = typedMaps.get(1);
            assertThat(row2.<Integer>getTyped("clust1")).isEqualTo(2);
            assertThat(row2.<Integer>getTyped("sum_val")).isEqualTo(2);

            final TypedMap row3 = typedMaps.get(2);
            assertThat(row3.<Integer>getTyped("clust1")).isEqualTo(3);
            assertThat(row3.<Integer>getTyped("sum_val")).isEqualTo(1);

            final TypedMap row4 = typedMaps.get(3);
            assertThat(row4.<Integer>getTyped("clust1")).isEqualTo(1);
            assertThat(row4.<Integer>getTyped("sum_val")).isEqualTo(2);

            final TypedMap row5 = typedMaps.get(4);
            assertThat(row5.<Integer>getTyped("clust1")).isEqualTo(2);
            assertThat(row5.<Integer>getTyped("sum_val")).isEqualTo(2);

            final TypedMap row6 = typedMaps.get(5);
            assertThat(row6.<Integer>getTyped("clust1")).isEqualTo(3);
            assertThat(row6.<Integer>getTyped("sum_val")).isEqualTo(1);

        } else {

            assertThat(row1.<Integer>getTyped("clust1")).isEqualTo(1);
            assertThat(row1.<Integer>getTyped("sum_val")).isEqualTo(2);

            final TypedMap row2 = typedMaps.get(1);
            assertThat(row2.<Integer>getTyped("clust1")).isEqualTo(2);
            assertThat(row2.<Integer>getTyped("sum_val")).isEqualTo(2);

            final TypedMap row3 = typedMaps.get(2);
            assertThat(row3.<Integer>getTyped("clust1")).isEqualTo(3);
            assertThat(row3.<Integer>getTyped("sum_val")).isEqualTo(1);

            final TypedMap row4 = typedMaps.get(3);
            assertThat(row4.<Integer>getTyped("clust1")).isEqualTo(1);
            assertThat(row4.<Integer>getTyped("sum_val")).isEqualTo(3);

            final TypedMap row5 = typedMaps.get(4);
            assertThat(row5.<Integer>getTyped("clust1")).isEqualTo(2);
            assertThat(row5.<Integer>getTyped("sum_val")).isEqualTo(2);

            final TypedMap row6 = typedMaps.get(5);
            assertThat(row6.<Integer>getTyped("clust1")).isEqualTo(3);
            assertThat(row6.<Integer>getTyped("sum_val")).isEqualTo(1);
        }


    }

    @Test
    public void should_select_using_token_function() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityForGroupBy/insert_multi_partitions.cql", ImmutableMap.of("id", id));

        //When
        TypedMap typedMap = manager
                .dsl()
                .select()
                .id()
                .function(SystemFunctions.token(COLUMNS.PARTITION_KEYS), "tokens")
                .fromBaseTable()
                .without_WHERE_Clause()
                .limit(1)
                .getTypedMap();

        //Then
        assertThat(typedMap).isNotNull();
        assertThat(typedMap).isNotEmpty();
        assertThat(typedMap.<Long>getTyped("tokens")).isNotNull();

    }

}
