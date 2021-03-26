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

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithPrimitiveClusteringColumn_Manager;
import info.archinnov.achilles.internals.entities.EntityWithPrimitiveClusteringColumn;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class PrimitiveAsClusteringColumnIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithPrimitiveClusteringColumn.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
            .build((cluster, statementsCache) ->
                    ManagerFactoryBuilder
                            .builder(cluster)
                            .withManagedEntityClasses(EntityWithPrimitiveClusteringColumn.class)
                            .doForceSchemaCreation(true)
                            .withStatementsCache(statementsCache)
                            .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                            .build()
            );

    private EntityWithPrimitiveClusteringColumn_Manager manager = resource.getManagerFactory().forEntityWithPrimitiveClusteringColumn();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    @Test
    public void should_select_dsl() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithPrimitiveClusteringColumn/insert_rows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithPrimitiveClusteringColumn> found = manager.
                dsl().
                select().
                allColumns_FromBaseTable().
                where().
                partition().Eq(id).
                clustering().IN(true, false).
                getList();

        //Then
        assertThat(found).hasSize(2);

        final EntityWithPrimitiveClusteringColumn falseE = found.get(0);
        assertThat(falseE.getClustering()).isFalse();
        assertThat(falseE.getValue()).isEqualTo("false");

        final EntityWithPrimitiveClusteringColumn trueE = found.get(1);
        assertThat(trueE.getClustering()).isTrue();
        assertThat(trueE.getValue()).isEqualTo("true");

    }

}
