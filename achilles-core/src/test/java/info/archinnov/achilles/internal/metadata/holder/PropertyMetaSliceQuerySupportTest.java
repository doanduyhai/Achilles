package info.archinnov.achilles.internal.metadata.holder;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaSliceQuerySupportTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EmbeddedIdProperties embeddedIdProperties;

    private PropertyMetaSliceQuerySupport view;

    @Before
    public void setUp() {
        view = new PropertyMetaSliceQuerySupport(meta);
        when(meta.getEmbeddedIdProperties()).thenReturn(embeddedIdProperties);
        when(meta.getEntityClassName()).thenReturn("entity");
    }

    @Test
    public void should_get_partition_key_names() throws Exception {
        //Given
        when(embeddedIdProperties.getPartitionComponents().getCQLComponentNames()).thenReturn(Arrays.asList("id","date", "type"));

        //When
        final List<String> partitionKeysName = view.getPartitionKeysName(2);

        //Then
        assertThat(partitionKeysName).containsExactly("id", "date");
    }

    @Test
    public void should_get_last_partition_key_name() throws Exception {
        //Given
        when(embeddedIdProperties.getPartitionComponents().getCQLComponentNames()).thenReturn(Arrays.asList("id","date", "type"));

        //When
        final String lastPartitionKeyName = view.getLastPartitionKeyName();

        //Then
        assertThat(lastPartitionKeyName).isEqualTo("type");
    }

    @Test
    public void should_get_clustering_key_names() throws Exception {
        //Given
        when(embeddedIdProperties.getClusteringComponents().getCQLComponentNames()).thenReturn(Arrays.asList("id","date", "type"));

        //When
        final List<String> clusteringKeysName = view.getClusteringKeysName(2);

        //Then
        assertThat(clusteringKeysName).containsExactly("id", "date");
    }

    @Test
    public void should_get_last_clustering_key_name() throws Exception {
        //Given
        when(embeddedIdProperties.getClusteringComponents().getCQLComponentNames()).thenReturn(Arrays.asList("id","date", "type"));

        //When
        final String lastClusteringKeyName = view.getLastClusteringKeyName();

        //Then
        assertThat(lastClusteringKeyName).isEqualTo("type");
    }

    @Test
    public void should_validate_partition_components() throws Exception {
        //Given
        final Object[] partitionComponents = {10L, "DuyHai"};

        //When
        view.validatePartitionComponents(partitionComponents);

        //Then
        verify(meta.getEmbeddedIdProperties().getPartitionComponents()).validatePartitionComponents("entity", partitionComponents);
    }

    @Test
    public void should_validate_partition_components_IN() throws Exception {
        //Given
        final Object[] partitionComponentsIN = {"Paul", "DuyHai"};

        //When
        view.validatePartitionComponentsIn(partitionComponentsIN);

        //Then
        verify(meta.getEmbeddedIdProperties().getPartitionComponents()).validatePartitionComponentsIn("entity", partitionComponentsIN);
    }

    @Test
    public void should_validate_clustering_components() throws Exception {
        //Given
        final Object[] clusteringComponents = {10L, "DuyHai"};

        //When
        view.validateClusteringComponents(clusteringComponents);

        //Then
        verify(meta.getEmbeddedIdProperties().getClusteringComponents()).validateClusteringComponents("entity", clusteringComponents);
    }

    @Test
    public void should_validate_clustering_components_IN() throws Exception {
        //Given
        final Object[] clusteringComponentsIN = {"Paul", "DuyHai"};

        //When
        view.validateClusteringComponentsIn(clusteringComponentsIN);

        //Then
        verify(meta.getEmbeddedIdProperties().getClusteringComponents()).validateClusteringComponentsIn("entity", clusteringComponentsIN);
    }

    @Test
    public void should_get_clustering_order() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("column", Sorting.DESC);
        when(meta.structure().isClustered()).thenReturn(true);
        when(meta.getEmbeddedIdProperties().getClusteringComponents().getClusteringOrders()).thenReturn(Arrays.asList(clusteringOrder));

        //When
        final ClusteringOrder actual = view.getClusteringOrder();

        //Then
        assertThat(actual).isSameAs(clusteringOrder);
    }
}