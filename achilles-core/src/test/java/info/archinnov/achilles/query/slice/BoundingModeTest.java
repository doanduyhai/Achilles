package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.query.slice.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.query.slice.BoundingMode.INCLUSIVE_BOUNDS;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class BoundingModeTest {

    List<String> clusteringKeyNames = Arrays.asList("col1","col2");

    @Test
         public void should_build_from_clustering_keys_ascending_inclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.ASC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        INCLUSIVE_BOUNDS.buildFromClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)>=(:col1,:col2);");
    }

    @Test
    public void should_build_from_clustering_keys_ascending_exclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.ASC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        EXCLUSIVE_BOUNDS.buildFromClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)>(:col1,:col2);");
    }


    @Test
    public void should_build_from_clustering_keys_descending_inclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.DESC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        INCLUSIVE_BOUNDS.buildFromClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)<=(:col1,:col2);");
    }

    @Test
    public void should_build_from_clustering_keys_descending_exclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.DESC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        EXCLUSIVE_BOUNDS.buildFromClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)<(:col1,:col2);");
    }



    @Test
    public void should_build_to_clustering_keys_ascending_inclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.ASC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        INCLUSIVE_BOUNDS.buildToClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)<=(:col1,:col2);");
    }

    @Test
    public void should_build_to_clustering_keys_ascending_exclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.ASC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        EXCLUSIVE_BOUNDS.buildToClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)<(:col1,:col2);");
    }


    @Test
    public void should_build_to_clustering_keys_descending_inclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.DESC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        INCLUSIVE_BOUNDS.buildToClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)>=(:col1,:col2);");
    }

    @Test
    public void should_build_to_clustering_keys_descending_exclusive() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("col1", Sorting.DESC);
        final Where where = QueryBuilder.select().from("table").where();

        //When
        EXCLUSIVE_BOUNDS.buildToClusteringKeys(where, clusteringOrder, clusteringKeyNames);

        //Then
        assertThat(where.toString()).isEqualToIgnoringCase("SELECT * FROM table WHERE (col1,col2)>(:col1,:col2);");
    }
}