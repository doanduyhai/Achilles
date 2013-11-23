package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.AchillesColumnMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

@RunWith(MockitoJUnitRunner.class)
public class ColumnMetaDataComparatorTest {

    private ColumnMetaDataComparator comparator = new ColumnMetaDataComparator();

    @Mock
    private TableMetadata tableMeta;

    private ColumnMetadata sourceColumnMetadata;

    private ColumnMetadata targetColumnMetadata;

    private String tableName = "table";

    @Before
    public void setUp() {
        when(tableMeta.getName()).thenReturn(tableName);
    }

    @Test
    public void should_compare_simple_type() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.text(), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.text(), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_compare_list_type() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.list(DataType.text()), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.list(DataType.text()), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_compare_set_type() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.set(DataType.text()), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.set(DataType.text()), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_compare_map_type() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.map(DataType.text(), DataType
                .text()), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.map(DataType.text(), DataType
                .text()), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_fail_if_not_same_column_name() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value1", DataType.set(DataType.text()), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value2", DataType.set(DataType.text()), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }

    @Test
    public void should_fail_if_not_same_table_name() throws Exception {
        //Given
        TableMetadata tableMeta2 = mock(TableMetadata.class);
        when(tableMeta2.getName()).thenReturn("table2");

        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.set(DataType.text()), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta2, "value", DataType.set(DataType.text()), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }

    @Test
    public void should_fail_if_not_same_column_type() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.text(), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.bigint(), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }

    @Test
    public void should_fail_if_not_same_list_parameter_type() throws Exception {
        //Given
        sourceColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.list(DataType.text()), null);
        targetColumnMetadata = new AchillesColumnMetadata(tableMeta, "value", DataType.list(DataType.bigint()), null);

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }
}
