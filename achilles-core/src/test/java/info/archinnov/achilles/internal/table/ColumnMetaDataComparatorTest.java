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

package info.archinnov.achilles.internal.table;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ColumnMetadataBuilder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

@RunWith(MockitoJUnitRunner.class)
@Ignore
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
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.text());
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.text());

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_compare_list_type() throws Exception {
        //Given
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.list(DataType.text()));
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.list(DataType.text()));

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_compare_set_type() throws Exception {
        //Given
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.set(DataType.text()));
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.set(DataType.text()));

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_compare_map_type() throws Exception {
        //Given
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.map(DataType.text(), DataType
                .text()));
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.map(DataType.text(), DataType
                .text()));

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isTrue();
    }

    @Test
    public void should_fail_if_not_same_column_name() throws Exception {
        //Given
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value1", DataType.set(DataType.text()));
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value2", DataType.set(DataType.text()));

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }

    @Test
    public void should_fail_if_not_same_table_name() throws Exception {
        //Given
        TableMetadata tableMeta2 = mock(TableMetadata.class);
        when(tableMeta2.getName()).thenReturn("table2");

        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.set(DataType.text()));
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta2, "value", DataType.set(DataType.text()));

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }

    @Test
    public void should_fail_if_not_same_column_type() throws Exception {
        //Given
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.text());
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.bigint());

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }

    @Test
    public void should_fail_if_not_same_list_parameter_type() throws Exception {
        //Given
        sourceColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.list(DataType.text()));
        targetColumnMetadata = ColumnMetadataBuilder.create(tableMeta, "value", DataType.list(DataType.bigint()));

        //Then
        assertThat(comparator.isEqual(sourceColumnMetadata, targetColumnMetadata)).isFalse();
    }
}
