package info.archinnov.achilles.internal.metadata.holder;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.DataType;
import info.archinnov.achilles.schemabuilder.Create;
import info.archinnov.achilles.schemabuilder.Create.Options;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTableCreatorTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EmbeddedIdProperties embeddedIdProperties;

    private PropertyMetaTableCreator view;

    @Before
    public void setUp() {
        view = new PropertyMetaTableCreator(meta);
        when(meta.getEmbeddedIdProperties()).thenReturn(embeddedIdProperties);
    }

    @Test
    public void should_add_partition_keys() throws Exception {
        //Given
        final Create create = mock(Create.class);
        PropertyMeta partitionMeta1 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta partitionMeta2 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(embeddedIdProperties.getPartitionComponents()).thenReturn(new PartitionComponents(asList(partitionMeta1, partitionMeta2)));

        when(partitionMeta1.getCQL3ColumnName()).thenReturn("id");
        when(partitionMeta2.getCQL3ColumnName()).thenReturn("name");

        when(partitionMeta1.structure().<Long>getCQL3ValueType()).thenReturn(Long.class);
        when(partitionMeta2.structure().<String>getCQL3ValueType()).thenReturn(String.class);

        //When
        view.addPartitionKeys(create);

        //Then
        InOrder inOrder = Mockito.inOrder(create);

        inOrder.verify(create).addPartitionKey("id", DataType.bigint());
        inOrder.verify(create).addPartitionKey("name", DataType.text());
    }

    @Test
    public void should_add_clustering_keys() throws Exception {
        //Given
        final Create create = mock(Create.class);
        PropertyMeta clusteringMeta1 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta clusteringMeta2 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(embeddedIdProperties.getClusteringComponents()).thenReturn(new ClusteringComponents(asList(clusteringMeta1, clusteringMeta2), Arrays.<ClusteringOrder>asList()));

        when(clusteringMeta1.getCQL3ColumnName()).thenReturn("id");
        when(clusteringMeta2.getCQL3ColumnName()).thenReturn("name");

        when(clusteringMeta1.structure().<Long>getCQL3ValueType()).thenReturn(Long.class);
        when(clusteringMeta2.structure().<String>getCQL3ValueType()).thenReturn(String.class);

        //When
        view.addClusteringKeys(create);

        //Then
        InOrder inOrder = Mockito.inOrder(create);

        inOrder.verify(create).addClusteringKey("id", DataType.bigint());
        inOrder.verify(create).addClusteringKey("name", DataType.text());
    }

    @Test
    public void should_create_new_index() throws Exception {
        //Given

        IndexProperties indexProperties = mock(IndexProperties.class, RETURNS_DEEP_STUBS);
        when(meta.getIndexProperties()).thenReturn(indexProperties);
        when(indexProperties.getIndexName()).thenReturn("name_index");
        when(meta.getCQL3ColumnName()).thenReturn("name");

        //When
        final String indexScript = view.createNewIndexScript("my_table");

        //Then
        assertThat(indexScript.trim()).isEqualTo("CREATE INDEX name_index ON my_table(name)");
    }

    @Test
    public void should_create_new_index_with_default_name() throws Exception {
        //Given

        IndexProperties indexProperties = mock(IndexProperties.class, RETURNS_DEEP_STUBS);
        when(meta.getIndexProperties()).thenReturn(indexProperties);
        when(indexProperties.getIndexName()).thenReturn(null);
        when(meta.getCQL3ColumnName()).thenReturn("name");

        //When
        final String indexScript = view.createNewIndexScript("my_table");

        //Then
        assertThat(indexScript.trim()).isEqualTo("CREATE INDEX my_table_name_idx ON my_table(name)");
    }

    @Test
    public void should_add_clustering_order() throws Exception {
        //Given
        ClusteringOrder clusteringOrder = new ClusteringOrder("date", Sorting.DESC);
        Create.Options tableOptions = mock(Create.Options.class);
        when(meta.structure().isClustered()).thenReturn(true);
        ArgumentCaptor<ClusteringOrder> clusteringOrdersCaptor = ArgumentCaptor.forClass(ClusteringOrder.class);

        when(embeddedIdProperties.getClusteringComponents().getClusteringOrders()).thenReturn(asList(clusteringOrder));
        when(tableOptions.clusteringOrder(clusteringOrdersCaptor.capture())).thenReturn(tableOptions);

        //When
        final Options actual = view.addClusteringOrder(tableOptions);

        //Then
        assertThat(actual).isSameAs(tableOptions);
        assertThat(clusteringOrdersCaptor.getValue()).isSameAs(clusteringOrder);
    }
}