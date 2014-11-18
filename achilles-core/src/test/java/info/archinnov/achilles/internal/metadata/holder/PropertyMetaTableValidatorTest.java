package info.archinnov.achilles.internal.metadata.holder;

import static com.datastax.driver.core.ColumnMetadataBuilder.create;
import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.text;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.type.Counter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTableValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EmbeddedIdProperties embeddedIdProperties;

    @Mock
    private TableMetadata tableMetadata;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

    @Mock
    private ConfigurationContext configContext;

    private PropertyMetaTableValidator view;

    @Before
    public void setUp() {
        view = new PropertyMetaTableValidator(meta);
        when(meta.getEmbeddedIdProperties()).thenReturn(embeddedIdProperties);
        when(tableMetadata.getName()).thenReturn("table");
    }

    @Test
    public void should_validate_partition_components() throws Exception {
        //Given
        final ColumnMetadata idColumnMeta = create(tableMetadata, "id", bigint());
        final ColumnMetadata nameColumnMeta = create(tableMetadata, "name", text());

        PropertyMeta partitionMeta1 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta partitionMeta2 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(embeddedIdProperties.getPartitionComponents()).thenReturn(new PartitionComponents(asList(partitionMeta1, partitionMeta2)));

        when(partitionMeta1.getCQL3ColumnName()).thenReturn("id");
        when(partitionMeta2.getCQL3ColumnName()).thenReturn("name");

        when(partitionMeta1.structure().<Long>getCQL3ValueType()).thenReturn(Long.class);
        when(partitionMeta2.structure().<String>getCQL3ValueType()).thenReturn(String.class);

        when(tableMetadata.getPartitionKey()).thenReturn(asList(idColumnMeta, nameColumnMeta));
        when(tableMetadata.getColumn("id")).thenReturn(idColumnMeta);
        when(tableMetadata.getColumn("name")).thenReturn(nameColumnMeta);

        //When
        view.validatePrimaryKeyComponents(tableMetadata, true);
    }

    @Test
    public void should_validate_clustering_components() throws Exception {
        //Given
        final ColumnMetadata idColumnMeta = create(tableMetadata, "id", bigint());
        final ColumnMetadata nameColumnMeta = create(tableMetadata, "name", text());

        PropertyMeta clusteringMeta1 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta clusteringMeta2 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(embeddedIdProperties.getClusteringComponents()).thenReturn(new ClusteringComponents(asList(clusteringMeta1, clusteringMeta2), Arrays.<ClusteringOrder>asList()));

        when(clusteringMeta1.getCQL3ColumnName()).thenReturn("id");
        when(clusteringMeta2.getCQL3ColumnName()).thenReturn("name");

        when(clusteringMeta1.structure().<Long>getCQL3ValueType()).thenReturn(Long.class);
        when(clusteringMeta2.structure().<String>getCQL3ValueType()).thenReturn(String.class);

        when(tableMetadata.getClusteringColumns()).thenReturn(asList(idColumnMeta, nameColumnMeta));
        when(tableMetadata.getColumn("id")).thenReturn(idColumnMeta);
        when(tableMetadata.getColumn("name")).thenReturn(nameColumnMeta);

        //When
        view.validatePrimaryKeyComponents(tableMetadata, false);
    }

    @Test
    public void should_validate_simple_column() throws Exception {
        //Given
        final ColumnMetadata nameColumnMeta = create(tableMetadata, "name", text());

        when(meta.getCQL3ColumnName()).thenReturn("name");
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(false);

        when(tableMetadata.getColumn("name")).thenReturn(nameColumnMeta);

        when(configContext.isRelaxIndexValidation()).thenReturn(true);

        //When
        view.validateColumn(tableMetadata, entityMeta, configContext);
    }

    @Test
    public void should_skip_simple_column_validation_if_dynamic_schema_update_enabled() throws Exception {
        //Given
        when(meta.getCQL3ColumnName()).thenReturn("name");
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(true);

        when(tableMetadata.getColumn("name")).thenReturn(null);

        //When
        view.validateColumn(tableMetadata, entityMeta, configContext);

        //Then
        verify(meta, never()).isStaticColumn();
        verifyZeroInteractions(configContext);
    }

    @Test
    public void should_verify_indexed_simple_column() throws Exception {
        //Given
        final ColumnMetadata nameColumnMeta = create(tableMetadata, "name", text());

        when(meta.getCQL3ColumnName()).thenReturn("name");
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(false);

        when(tableMetadata.getColumn("name")).thenReturn(nameColumnMeta);

        when(configContext.isRelaxIndexValidation()).thenReturn(false);

        when(meta.structure().isIndexed()).thenReturn(true);

        //When

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' in the table '%s' is indexed (or not) whereas metadata indicates it is (or not)","name","table"));

        view.validateColumn(tableMetadata, entityMeta, configContext);
    }

    @Test
    public void should_validate_list_column() throws Exception {
        //Given
        final ColumnMetadata listColumnMeta = create(tableMetadata, "list", DataType.list(DataType.text()));

        when(meta.getCQL3ColumnName()).thenReturn("list");
        when(meta.type()).thenReturn(LIST);
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(false);

        when(tableMetadata.getColumn("list")).thenReturn(listColumnMeta);

        //When
        view.validateCollectionAndMapColumn(tableMetadata, entityMeta);
    }

    @Test
    public void should_validate_set_column() throws Exception {
        //Given
        final ColumnMetadata setColumnMeta = create(tableMetadata, "set", DataType.set(DataType.text()));

        when(meta.getCQL3ColumnName()).thenReturn("set");
        when(meta.type()).thenReturn(SET);
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(false);

        when(tableMetadata.getColumn("set")).thenReturn(setColumnMeta);

        //When
        view.validateCollectionAndMapColumn(tableMetadata, entityMeta);
    }

    @Test
    public void should_validate_map_column() throws Exception {
        //Given
        final ColumnMetadata mapColumnMeta = create(tableMetadata, "map", DataType.map(DataType.cint(), DataType.text()));

        when(meta.getCQL3ColumnName()).thenReturn("map");
        when(meta.type()).thenReturn(MAP);
        when(meta.structure().<Integer>getCQL3KeyType()).thenReturn(Integer.class);
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(false);

        when(tableMetadata.getColumn("map")).thenReturn(mapColumnMeta);

        //When
        view.validateCollectionAndMapColumn(tableMetadata, entityMeta);
    }

    @Test
    public void should_skip_validation_of_list_column_if_schema_update_enabled() throws Exception {
        //Given
        when(meta.getCQL3ColumnName()).thenReturn("list");
        when(meta.type()).thenReturn(LIST);
        when(meta.structure().<String>getCQL3ValueType()).thenReturn(String.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(true);

        when(tableMetadata.getColumn("list")).thenReturn(null);

        //When
        view.validateCollectionAndMapColumn(tableMetadata, entityMeta);
    }

    @Test
    public void should_validate_clustered_counter_column() throws Exception {
        final ColumnMetadata counterColumnMeta = create(tableMetadata, "count", counter());

        when(meta.getCQL3ColumnName()).thenReturn("count");
        when(meta.structure().<Counter>getCQL3ValueType()).thenReturn(Counter.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(false);

        when(tableMetadata.getColumn("count")).thenReturn(counterColumnMeta);

        //When
        view.validateClusteredCounterColumn(tableMetadata, entityMeta);
    }

    @Test
    public void should_skip_validation_of_clustered_counter_column_if_schema_update_enabled() throws Exception {
        when(meta.getCQL3ColumnName()).thenReturn("count");
        when(meta.structure().<Counter>getCQL3ValueType()).thenReturn(Counter.class);
        when(meta.isStaticColumn()).thenReturn(false);
        when(entityMeta.config().isSchemaUpdateEnabled()).thenReturn(true);

        when(tableMetadata.getColumn("count")).thenReturn(null);

        //When
        view.validateClusteredCounterColumn(tableMetadata, entityMeta);
    }
}