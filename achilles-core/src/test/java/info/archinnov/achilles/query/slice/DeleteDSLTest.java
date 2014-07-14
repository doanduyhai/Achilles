/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.query.slice;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.schemabuilder.Create;

@RunWith(MockitoJUnitRunner.class)
public class DeleteDSLTest {

    @Mock
    private SliceQueryExecutor executor;

    @Mock
    private EntityMeta entityMeta;

    private Delete delete = QueryBuilder.delete().from("table");

    @Before
    public void setUp() {
        when(entityMeta.getClusteringOrders()).thenReturn(asList(new Create.Options.ClusteringOrder("col1", Create.Options.ClusteringOrder.Sorting.ASC)));
        when(entityMeta.getPartitionKeysSize()).thenReturn(2);
        when(entityMeta.getClusteringKeysSize()).thenReturn(3);

        when(entityMeta.getPartitionKeysName(1)).thenReturn(asList("id"));
        when(entityMeta.getLastPartitionKeyName()).thenReturn("bucket");
        when(entityMeta.getClusteringKeysName(2)).thenReturn(asList("col1", "col2"));
        when(entityMeta.getClusteringKeysName(3)).thenReturn(asList("col1", "col2", "col3"));
        when(entityMeta.getLastClusteringKeyName()).thenReturn("col3");
    }

    @Test
    public void should_delete_with_partition_keys_only() throws Exception {
        //Given
        final DeleteDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forDelete();

        //When
        final DeleteFromPartition<String> start = builder.withPartitionComponents("a");

        start.delete();

        final Delete.Where whereClause = start.properties.generateWhereClauseForDelete(delete);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("DELETE FROM table WHERE id=:id;");
        assertThat(start.properties.getBoundValues()).containsSequence("a");
    }

    @Test
    public void should_delete_with_partition_keys_IN() throws Exception {
        //Given
        final DeleteDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forDelete();

        //When
        final DeleteWithPartition<String> start = builder.withPartitionComponentsIN("a", "b");

        start.delete();

        final Delete.Where whereClause = start.properties.generateWhereClauseForDelete(delete);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("DELETE FROM table WHERE bucket IN :partitionComponentsIn;");
        assertThat(start.properties.getBoundValues()).containsSequence(asList("a","b"));
    }

    @Test
    public void should_delete_with_matching_clustering_keys() throws Exception {
        //Given
        final DeleteDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forDelete();

        //When
        final DeleteFromPartition<String> start = builder.withPartitionComponents("a");

        start.deleteMatching("A", "B");

        final Delete.Where whereClause = start.properties.generateWhereClauseForDelete(delete);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("DELETE FROM table WHERE id=:id AND col1=:col1 AND col2=:col2;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B");
    }

}
