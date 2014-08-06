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

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
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

@RunWith(MockitoJUnitRunner.class)
public class SelectDSLTest {

    @Mock
    private SliceQueryExecutor executor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

    private Select select = QueryBuilder.select().from("table");

    @Before
    public void setUp() {
        when(entityMeta.forSliceQuery().getClusteringOrderForSliceQuery()).thenReturn(new ClusteringOrder("col1", Sorting.ASC));
        when(entityMeta.forSliceQuery().getPartitionKeysSize()).thenReturn(2);
        when(entityMeta.forSliceQuery().getClusteringKeysSize()).thenReturn(3);

        when(entityMeta.forSliceQuery().getPartitionKeysName(1)).thenReturn(asList("id"));
        when(entityMeta.forSliceQuery().getLastPartitionKeyName()).thenReturn("bucket");
        when(entityMeta.forSliceQuery().getClusteringKeysName(2)).thenReturn(asList("col1", "col2"));
        when(entityMeta.forSliceQuery().getClusteringKeysName(3)).thenReturn(asList("col1", "col2", "col3"));
        when(entityMeta.forSliceQuery().getLastClusteringKeyName()).thenReturn("col3");
    }

    @Test
    public void should_get_with_limit_from_partition_keys_only() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder
                .withPartitionComponents("a");

        start.limit(3).get(10);

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", 10);
    }

    @Test
    public void should_get_from_partition_keys_IN() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectWithPartition<String> start = builder
                .withPartitionComponentsIN("a", "b");

        start.limit(2).fromClusterings("A","B").limit(3).get(10);

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE bucket IN :partitionComponentsIn AND (col1,col2)>=(:col1,:col2) ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence(asList("a","b"), "A","B",10);
    }

    @Test
    public void should_get_one_from_partition_keys_only() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder
                .withPartitionComponents("a");

        start.limit(3).getOne();

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", 1);
    }

    @Test
    public void should_get_with_limit_with_partition_keys_and_clustering_keys() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder
                .withPartitionComponents("a");

        start.andPartitionComponentsIN("b", "c")
            .limit(3)
            .withConsistency(QUORUM)
            .fromClusterings("A", "B")
            .toClusterings("C", "D")
            .get(10);

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND bucket IN :partitionComponentsIn AND (col1,col2)>=(:col1,:col2) AND (col1,col2)<=(:col1,:col2) ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", asList("b", "c"), "A", "B", "C", "D", 10);
    }

    @Test
    public void should_get_one_with_clustering_keys_IN() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder
                .withPartitionComponents("a");

        start.limit(3)
                .withClusterings("A", "B")
                .andClusteringsIN("C", "D")
                .getOne();

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 AND col3 IN :clusteringKeysIn ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", asList("C", "D"), 1);
    }

    @Test
    public void should_get_one_with_from_clustering_only() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).fromClusterings("A", "B").fromExclusiveToInclusiveBounds().orderByDescending().getOne();

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND (col1,col2)>(:col1,:col2) ORDER BY col1 DESC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", 1);
    }

    @Test
    public void should_get_matching_clustering_keys() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).getMatching("A", "B", "C");

        System.out.println("start.properties = " + start.properties);

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 AND col3=:col3 ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", "C", 3);
    }


    @Test
    public void should_get_first_matching_clustering_keys() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).getFirstMatching(5, "A", "B", "C");

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 AND col3=:col3 ORDER BY col1 ASC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", "C", 5);
    }

    @Test
    public void should_get_last_matching_clustering_keys() throws Exception {
        //Given
        final SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        //When
        final SelectFromPartition<String> start = builder.withPartitionComponents("a");

        start.limit(3).getLastMatching(5, "A", "B", "C");

        final RegularStatement whereClause = start.properties.generateWhereClauseForSelect(select);

        //Then
        assertThat(whereClause.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND col1=:col1 AND col2=:col2 AND col3=:col3 ORDER BY col1 DESC LIMIT :limitSize;");
        assertThat(start.properties.getBoundValues()).containsSequence("a", "A", "B", "C", 5);
    }
}
