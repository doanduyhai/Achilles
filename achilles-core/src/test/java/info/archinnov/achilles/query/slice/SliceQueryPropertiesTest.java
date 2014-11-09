package info.archinnov.achilles.query.slice;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.query.slice.SliceQueryProperties.SliceType;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryPropertiesTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Before
    public void setUp() {
        when(meta.forSliceQuery().getClusteringOrderForSliceQuery()).thenReturn(new ClusteringOrder("date", Sorting.DESC));
    }

    @Test
    public void should_generate_where_clause_for_select_with_partitions_IN_and_clusterings_IN() throws Exception {
        //Given
        final List<Object> partitionKeys = Arrays.<Object>asList(10L, 1);
        final List<Object> partitionKeysIN = Arrays.<Object>asList(2013, 2014);

        final List<Object> clusteringKeys = Arrays.<Object>asList(20140830);
        final List<Object> clusteringKeysIN = Arrays.<Object>asList(PropertyType.LIST, PropertyType.SET);

        final Select select = select().from("table");

        //When
        final RegularStatement statement = SliceQueryProperties.builder(meta, ClusteredEntity.class, SliceType.SELECT)
                .partitionKeysName(asList("id", "bucket")).lastPartitionKeyName("year")
                .partitionKeys(partitionKeys).andPartitionKeysIn(partitionKeysIN)
                .withClusteringKeysName(asList("date")).lastClusteringKeyName("type")
                .withClusteringKeys(clusteringKeys).andClusteringKeysIn(clusteringKeysIN)
                .ordering(OrderingMode.DESCENDING)
                .limit(12)
                .generateWhereClauseForSelect(select);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND bucket=:bucket AND year IN :partitionComponentsIn AND date=:date AND type IN :clusteringKeysIn ORDER BY date DESC LIMIT :limitSize;");
    }

    @Test
    public void should_generate_where_clause_for_select_with_partitions_and_clusterings_from_to_exclusives() throws Exception {
        //Given
        final List<Object> partitionKeys = Arrays.<Object>asList(10L, 1);

        final List<Object> clusteringKeys = Arrays.<Object>asList(20140830, PropertyType.COUNTER);

        final Select select = select().from("table");

        //When
        final RegularStatement statement = SliceQueryProperties.builder(meta, ClusteredEntity.class, SliceType.SELECT)
                .partitionKeysName(asList("id", "bucket")).partitionKeys(partitionKeys)
                .fromClusteringKeysName(asList("date")).fromClusteringKeys(clusteringKeys)
                .toClusteringKeysName(asList("date", "type")).toClusteringKeys(clusteringKeys)
                .bounding(BoundingMode.EXCLUSIVE_BOUNDS)
                .limit(12)
                .generateWhereClauseForSelect(select);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND bucket=:bucket AND (date)<(:date) AND (date,type)>(:date,:type) LIMIT :limitSize;");
    }

    @Test
    public void should_generate_where_clause_for_select_with_partitions_and_clusterings_from_to_inclusive() throws Exception {
        //Given
        final List<Object> partitionKeys = Arrays.<Object>asList(10L, 1);

        final List<Object> clusteringKeys = Arrays.<Object>asList(20140830, PropertyType.COUNTER);

        final Select select = select().from("table");

        //When
        final RegularStatement statement = SliceQueryProperties.builder(meta, ClusteredEntity.class, SliceType.SELECT)
                .partitionKeysName(asList("id", "bucket")).partitionKeys(partitionKeys)
                .fromClusteringKeysName(asList("date")).fromClusteringKeys(clusteringKeys)
                .toClusteringKeysName(asList("date", "type")).toClusteringKeys(clusteringKeys)
                .bounding(BoundingMode.INCLUSIVE_BOUNDS)
                .ordering(null)
                .limit(12)
                .generateWhereClauseForSelect(select);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND bucket=:bucket AND (date)<=(:date) AND (date,type)>=(:date,:type) LIMIT :limitSize;");
    }

    @Test
    public void should_generate_where_clause_for_delete() throws Exception {
        final List<Object> partitionKeys = Arrays.<Object>asList(10L, 1);
        final List<Object> partitionKeysIN = Arrays.<Object>asList(2013, 2014);

        final List<Object> clusteringKeys = Arrays.<Object>asList(20140830, PropertyType.COUNTER);

        final Delete delete = delete().from("table");

        //When
        final RegularStatement statement = SliceQueryProperties.builder(meta, ClusteredEntity.class, SliceType.SELECT)
                .partitionKeysName(asList("id", "bucket")).lastPartitionKeyName("year")
                .partitionKeys(partitionKeys).andPartitionKeysIn(partitionKeysIN)
                .withClusteringKeysName(asList("date", "type"))
                .withClusteringKeys(clusteringKeys)
                .generateWhereClauseForDelete(delete);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("DELETE FROM table WHERE id=:id AND bucket=:bucket AND year IN :partitionComponentsIn AND date=:date AND type=:type;");
    }

    @Test
    public void should_get_bound_values_with_partitions_and_clusterings_IN() throws Exception {
        //Given
        final List<Object> partitionKeys = Arrays.<Object>asList(10L, 1);
        final List<Object> partitionKeysIN = Arrays.<Object>asList(2013, 2014);

        final List<Object> clusteringKeys = Arrays.<Object>asList(20140830);
        final List<Object> clusteringKeysIN = Arrays.<Object>asList(PropertyType.LIST, PropertyType.SET);

        when(meta.forTranscoding().encodePartitionComponents(partitionKeys)).thenReturn(partitionKeys);
        when(meta.forTranscoding().encodePartitionComponentsIN(partitionKeysIN)).thenReturn(partitionKeysIN);
        when(meta.forTranscoding().encodeClusteringKeys(clusteringKeys)).thenReturn(clusteringKeys);
        when(meta.forTranscoding().encodeClusteringKeysIN(clusteringKeysIN)).thenReturn(clusteringKeysIN);

        //When
        final Object[] boundValues = SliceQueryProperties.builder(meta, ClusteredEntity.class, SliceType.SELECT)
                .partitionKeys(partitionKeys).andPartitionKeysIn(partitionKeysIN)
                .withClusteringKeys(clusteringKeys).andClusteringKeysIn(clusteringKeysIN)
                .limit(12)
                .getBoundValues();
        //Then
        assertThat(asList(boundValues)).containsExactly(10L, 1, asList(2013, 2014), 20140830, asList(PropertyType.LIST, PropertyType.SET), 12);
    }

    @Test
    public void should_get_bound_values_with_partitions_and_clusterings_from_to() throws Exception {
        //Given
        final List<Object> partitionKeys = Arrays.<Object>asList(10L, 1);

        final List<Object> fromClusteringKeys = Arrays.<Object>asList(20140830, PropertyType.LIST);
        final List<Object> toClusteringKeys = Arrays.<Object>asList(20140830, PropertyType.SET);

        when(meta.forTranscoding().encodePartitionComponents(partitionKeys)).thenReturn(partitionKeys);
        when(meta.forTranscoding().encodeClusteringKeys(fromClusteringKeys)).thenReturn(fromClusteringKeys);
        when(meta.forTranscoding().encodeClusteringKeys(toClusteringKeys)).thenReturn(toClusteringKeys);

        //When
        final Object[] boundValues = SliceQueryProperties.builder(meta, ClusteredEntity.class, SliceType.SELECT)
                .partitionKeys(partitionKeys)
                .fromClusteringKeys(fromClusteringKeys).toClusteringKeys(toClusteringKeys)
                .limit(12)
                .getBoundValues();
        //Then
        assertThat(asList(boundValues)).containsExactly(10L, 1, 20140830, PropertyType.LIST, 20140830, PropertyType.SET, 12);
    }
}