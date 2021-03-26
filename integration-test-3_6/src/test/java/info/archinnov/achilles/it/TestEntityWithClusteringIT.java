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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_6;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_6;
import info.archinnov.achilles.generated.manager.EntityWithClustering_Manager;
import info.archinnov.achilles.internals.entities.EntityWithClustering;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithClusteringIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_6> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_6")
            .entityClassesToTruncate(EntityWithClustering.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_6
                    .builder(cluster)
                    .withDefaultKeyspaceName("it_3_6")
                    .withManagedEntityClasses(EntityWithClustering.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithClustering_Manager manager = resource.getManagerFactory().forEntityWithClustering();

    @Test
    public void should_select_with_allow_per_partition_limit() throws Exception {
        //Given
        Long id1 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        Long id2 = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        Long id3 = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithClustering/insertRows.cql", ImmutableMap.of("id1", id1, "id2", id2, "id3", id3));

        //When
        final List<EntityWithClustering> list = manager
                .dsl()
                .select()
                .allColumns_FromBaseTable()
                .without_WHERE_Clause()
                .perPartitionLimit(2)
                .getList();

        //Then
        assertThat(list).hasSize(6);
        assertThat(list.stream().map(x -> x.getClust()).collect(Collectors.toList())).containsExactly(1L, 2L, 1L, 2L, 1L, 2L);
    }

}
