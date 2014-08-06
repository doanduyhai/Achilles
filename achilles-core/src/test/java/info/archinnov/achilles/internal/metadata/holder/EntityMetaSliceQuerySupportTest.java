package info.archinnov.achilles.internal.metadata.holder;

import static java.util.Arrays.asList;
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
public class EntityMetaSliceQuerySupportTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    private EntityMetaSliceQuerySupport view;

    @Before
    public void setUp() {
        view = new EntityMetaSliceQuerySupport(meta);
    }

    @Test
    public void should_validate_partition_components() throws Exception {
        //Given
        Object[] components = new Object[]{10L};

        //When
        view.validatePartitionComponents(components);

        //Then
        verify(meta.getIdMeta().forSliceQuery()).validatePartitionComponents(components);
    }

    @Test
    public void should_validate_partition_components_IN() throws Exception {
        //Given
        Object[] components = new Object[]{10L};

        //When
        view.validatePartitionComponentsIn(components);

        //Then
        verify(meta.getIdMeta().forSliceQuery()).validatePartitionComponentsIn(components);
    }


    @Test
    public void should_validate_clustering_components() throws Exception {
        //Given
        Object[] components = new Object[]{10L};

        //When
        view.validateClusteringComponents(components);

        //Then
        verify(meta.getIdMeta().forSliceQuery()).validateClusteringComponents(components);
    }

    @Test
    public void should_validate_clustering_components_IN() throws Exception {
        //Given
        Object[] components = new Object[]{10L};

        //When
        view.validateClusteringComponentsIn(components);

        //Then
        verify(meta.getIdMeta().forSliceQuery()).validateClusteringComponentsIn(components);
    }

    @Test
    public void should_get_partition_key_names() throws Exception {
        //Given
        final List<String> names = asList("id", "bucket");
        when(meta.getIdMeta().forSliceQuery().getPartitionKeysName(2)).thenReturn(names);

        //When
        final List<String> actual = view.getPartitionKeysName(2);

        //Then
        assertThat(actual).containsExactly("id", "bucket");
    }


    @Test
    public void should_get_last_partition_key_name() throws Exception {
        //Given
        when(meta.getIdMeta().forSliceQuery().getLastPartitionKeyName()).thenReturn("bucket");

        //When
        final String actual = view.getLastPartitionKeyName();

        //Then
        assertThat(actual).isEqualTo("bucket");
    }

    @Test
    public void should_get_clustering_key_names() throws Exception {
        //Given
        final List<String> names = asList("id", "bucket");
        when(meta.getIdMeta().forSliceQuery().getClusteringKeysName(2)).thenReturn(names);

        //When
        final List<String> actual = view.getClusteringKeysName(2);

        //Then
        assertThat(actual).containsExactly("id", "bucket");
    }


    @Test
    public void should_get_last_clustering_key_name() throws Exception {
        //Given
        when(meta.getIdMeta().forSliceQuery().getLastClusteringKeyName()).thenReturn("bucket");

        //When
        final String actual = view.getLastClusteringKeyName();

        //Then
        assertThat(actual).isEqualTo("bucket");
    }

    @Test
    public void should_get_partition_key_size() throws Exception {
        //Given
        when(meta.getIdMeta().forSliceQuery().getPartitionKeysSize()).thenReturn(2);

        //When
        final int actual = view.getPartitionKeysSize();

        //Then
        assertThat(actual).isEqualTo(2);
    }

    @Test
    public void should_get_clustering_key_size() throws Exception {
        //Given
        when(meta.getIdMeta().forSliceQuery().getClusteringKeysSize()).thenReturn(2);

        //When
        final int actual = view.getClusteringKeysSize();

        //Then
        assertThat(actual).isEqualTo(2);
    }

    @Test
    public void should_get_clustering_order() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("name", Sorting.DESC);
        when(meta.getIdMeta().forSliceQuery().getClusteringOrder()).thenReturn(clusteringOrder);

        //When
        final ClusteringOrder actual = view.getClusteringOrderForSliceQuery();

        //Then
        assertThat(actual).isSameAs(clusteringOrder);
    }
}