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
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.schemabuilder.Create;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class IterateDSLTest {

    @Mock
    private SliceQueryExecutor executor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    private Select select = QueryBuilder.select().from("table");

    @Before
    public void setUp() {
        when(meta.forSliceQuery().getClusteringOrderForSliceQuery()).thenReturn(new Create.Options.ClusteringOrder("col1", Create.Options.ClusteringOrder.Sorting.ASC));
        when(meta.forSliceQuery().getPartitionKeysSize()).thenReturn(2);
        when(meta.forSliceQuery().getClusteringKeysSize()).thenReturn(3);

        when(meta.forSliceQuery().getPartitionKeysName(1)).thenReturn(asList("id"));
        when(meta.forSliceQuery().getLastPartitionKeyName()).thenReturn("bucket");
        when(meta.forSliceQuery().getClusteringKeysName(2)).thenReturn(asList("col1", "col2"));
        when(meta.forSliceQuery().getClusteringKeysName(3)).thenReturn(asList("col1", "col2", "col3"));
        when(meta.forSliceQuery().getLastClusteringKeyName()).thenReturn("col3");
    }

    @Test
    public void should_iterate() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, meta).forIteration();
        when(meta.forTranscoding().encodePartitionComponents(Arrays.<Object>asList("a"))).thenReturn(Arrays.<Object>asList("a"));

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iterator();

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", 3);
        assertThat(start.properties.fetchSizeO.isPresent()).isFalse();
    }

    @Test
    public void should_iterate_with_partition_keys_IN() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, meta).forIteration();
        when(meta.forTranscoding().encodePartitionComponents(asList())).thenReturn(asList());
        when(meta.forTranscoding().encodePartitionComponentsIN(Arrays.<Object>asList("a", "b"))).thenReturn(Arrays.<Object>asList("a", "b"));
        when(meta.forTranscoding().encodeClusteringKeys(Arrays.<Object>asList("A", "B"))).thenReturn(Arrays.<Object>asList("A", "B"));

        //When
        final IterateWithPartition<String> start = builder.withPartitionComponentsIN("a", "b");

        start.fromClusterings("A", "B").limit(3).iterator(10);

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE bucket IN :partitionComponentsIn AND (col1,col2)>=(:col1,:col2) LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence(asList("a", "b"), "A", "B", 3);
        assertThat(start.properties.fetchSizeO.get()).isEqualTo(10);
    }

    @Test
    public void should_iterate_with_batchsize() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, meta).forIteration();
        when(meta.forTranscoding().encodePartitionComponents(Arrays.<Object>asList("a"))).thenReturn(Arrays.<Object>asList("a"));

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iterator(120);

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", 3);
        assertThat(start.properties.fetchSizeO.get()).isEqualTo(120);
    }

    @Test
    public void should_iterate_with_matching() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, meta).forIteration();
        when(meta.forTranscoding().encodePartitionComponents(Arrays.<Object>asList("a"))).thenReturn(Arrays.<Object>asList("a"));
        when(meta.forTranscoding().encodeClusteringKeys(Arrays.<Object>asList("A", "B"))).thenReturn(Arrays.<Object>asList("A" , "B"));

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iteratorWithMatching("A", "B");

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", 3);
        assertThat(start.properties.fetchSizeO.isPresent()).isFalse();
    }

    @Test
    public void should_iterate_with_matching_and_batchsize() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, meta).forIteration();
        when(meta.forTranscoding().encodePartitionComponents(Arrays.<Object>asList("a"))).thenReturn(Arrays.<Object>asList("a"));
        when(meta.forTranscoding().encodeClusteringKeys(Arrays.<Object>asList("A", "B"))).thenReturn(Arrays.<Object>asList("A" , "B"));

        //When
        final IterateFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).iteratorWithMatchingAndBatchSize(123, "A", "B");

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", 3);
        assertThat(start.properties.fetchSizeO.get()).isEqualTo(123);
    }

}
