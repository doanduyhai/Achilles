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

import static info.archinnov.achilles.query.slice.SliceQueryProperties.DEFAULT_BATCH_SIZE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.schemabuilder.Create;

@RunWith(MockitoJUnitRunner.class)
public class IterateDSLTest {

    @Mock
    private SliceQueryExecutor executor;

    @Mock
    private EntityMeta entityMeta;

    private Select select = QueryBuilder.select().from("table");

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
    public void should_iterate() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iterator();

        final Select whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", 3);
        assertThat(start.properties.batchSize).isEqualTo(DEFAULT_BATCH_SIZE);
    }

    @Test
    public void should_iterate_with_partition_keys_IN() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        //When
        final IterateWithPartition<String> start = builder.withPartitionComponentsIN("a", "b");

        start.fromClusterings("A", "B").limit(3).iterator();

        final Select whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE bucket IN :partitionComponentsIn AND (col1,col2)>=(:col1,:col2) ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence(asList("a", "b"), "A", "B", 3);
        assertThat(start.properties.batchSize).isEqualTo(DEFAULT_BATCH_SIZE);
    }

    @Test
    public void should_iterate_with_batchsize() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iterator(120);

        final Select whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", 3);
        assertThat(start.properties.batchSize).isEqualTo(120);
    }

    @Test
    public void should_iterate_with_matching() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iteratorWithMatching("A", "B");

        final Select whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", 3);
        assertThat(start.properties.batchSize).isEqualTo(DEFAULT_BATCH_SIZE);
    }

    @Test
    public void should_iterate_with_matching_and_batchsize() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iteratorWithMatchingAndBatchSize(123, "A", "B");

        final Select whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", 3);
        assertThat(start.properties.batchSize).isEqualTo(123);
    }

}
