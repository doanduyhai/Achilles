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
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_0;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_0;
import info.archinnov.achilles.generated.manager.MultiClusteringEntity_Manager;
import info.archinnov.achilles.internals.entities.MultiClusteringEntity;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestMultiClusteringEntityIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_0> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(MultiClusteringEntity.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_0
                    .builder(cluster)
                    .withManagedEntityClasses(MultiClusteringEntity.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private Session session = resource.getNativeSession();
    private MultiClusteringEntity_Manager manager = resource.getManagerFactory().forMultiClusteringEntity();
    
    @Test
    public void should_update_with_IN_clause_on_all_clusterings() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new val")
                .where()
                .id().Eq(id)
                .c1().IN(1,3)
                .c2().IN(1,2)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 IN(1,3) AND c2 IN (1,2)").all();

        assertThat(all).hasSize(4);
        assertThat(all.stream().map(x -> x.getString("value")).collect(Collectors.toList()))
                .containsExactly("new val", "new val", "new val", "new val");
    }

    @Test
    public void should_update_with_IN_clause_on_first_clustering() throws Exception {
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new val")
                .where()
                .id().Eq(id)
                .c1().IN(1,3)
                .c2().Eq(1)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 IN(1,3) AND c2=1").all();

        assertThat(all).hasSize(2);
        assertThat(all.stream().map(x -> x.getString("value")).collect(Collectors.toList()))
                .containsExactly("new val", "new val");
    }

    @Test
    public void should_update_with_IN_clause_on_last_clustering() throws Exception {
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .value().Set("new val")
                .where()
                .id().Eq(id)
                .c1().Eq(1)
                .c2().IN(1,2)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 = 1 AND c2 IN(1,2)").all();

        assertThat(all).hasSize(2);
        assertThat(all.stream().map(x -> x.getString("value")).collect(Collectors.toList()))
                .containsExactly("new val", "new val");
    }

    @Test
    public void should_delete_with_IN_clause_on_all_clusterings() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().IN(1, 2)
                .c2().IN(1, 2)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 IN(1, 2) AND c2 IN(1, 2)").all();

        assertThat(all).isEmpty();
    }

    @Test
    public void should_delete_with_IN_clause_on_first_clustering() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().IN(1, 2)
                .c2().Eq(1)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 IN(1, 2) AND c2 = 1").all();

        assertThat(all).isEmpty();
    }


    @Test
    public void should_delete_with_IN_clause_on_last_clustering() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().Eq(1)
                .c2().IN(1, 2)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 = 1 AND c2 IN(1, 2)").all();

        assertThat(all).isEmpty();
    }

    @Test
    public void should_delete_with_slice_on_last_clustering() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().Eq(1)
                .c2().Gte_And_Lte(1, 2)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 = 1 AND c2 >= 1 AND c2 <= 2").all();

        assertThat(all).isEmpty();
    }


    @Test
    public void should_delete_with_slice_on_first_clustering() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().Gte_And_Lte(1, 2)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND c1 >= 1 AND c1 <= 2").all();

        assertThat(all).isEmpty();
    }

    @Test
    public void should_delete_with_multi_columns_slice() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1_c2().Gte_And_Lte(1, 1, 3, 1)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id
                + " AND (c1,c2) >= (1,1) AND (c1,c2) <= (3,1)").all();

        assertThat(all).isEmpty();
    }

    @Test
    public void should_delete_by_partition_only() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .execute();

        final List<Row> all = session.execute("SELECT value FROM achilles_embedded.multi_clustering_entity WHERE id = " + id).all();

        //Then
        assertThat(all).isEmpty();
    }

    @Test
    public void should_delete_by_partition_and_first_clustering_column_only() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().Eq(3)
                .execute();

        final List<Row> all = session.execute("SELECT c1,c2 FROM achilles_embedded.multi_clustering_entity WHERE id = " + id).all();

        //Then
        assertThat(all).hasSize(6);
        assertThat(all.get(0).getInt("c1")).isEqualTo(1);
        assertThat(all.get(0).getInt("c2")).isEqualTo(1);
        assertThat(all.get(1).getInt("c1")).isEqualTo(1);
        assertThat(all.get(1).getInt("c2")).isEqualTo(2);
        assertThat(all.get(2).getInt("c1")).isEqualTo(1);
        assertThat(all.get(2).getInt("c2")).isEqualTo(3);

        assertThat(all.get(3).getInt("c1")).isEqualTo(2);
        assertThat(all.get(3).getInt("c2")).isEqualTo(1);
        assertThat(all.get(4).getInt("c1")).isEqualTo(2);
        assertThat(all.get(4).getInt("c2")).isEqualTo(2);
        assertThat(all.get(5).getInt("c1")).isEqualTo(2);
        assertThat(all.get(5).getInt("c2")).isEqualTo(3);
    }

    @Test
    public void should_delete_by_partition_and_first_clustering_column_range() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("MultiClusteringEntity/insertRows.cql", ImmutableMap.of("id", id));

        //When
        manager.dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .c1().Gte_And_Lte(1, 2)
                .execute();

        final List<Row> all = session.execute("SELECT c1,c2 FROM achilles_embedded.multi_clustering_entity WHERE id = " + id).all();

        //Then
        assertThat(all).hasSize(3);

        assertThat(all.get(0).getInt("c1")).isEqualTo(3);
        assertThat(all.get(0).getInt("c2")).isEqualTo(1);
        assertThat(all.get(1).getInt("c1")).isEqualTo(3);
        assertThat(all.get(1).getInt("c2")).isEqualTo(2);
        assertThat(all.get(2).getInt("c1")).isEqualTo(3);
        assertThat(all.get(2).getInt("c2")).isEqualTo(3);
    }
}
