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

import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.inet;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;
import static com.datastax.driver.core.DataType.text;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COMPOUND_PRIMARY_KEY;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.PARTITION_KEY;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.valueClass;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import com.datastax.driver.core.ColumnMetadataBuilder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import info.archinnov.achilles.test.parser.entity.CompoundPK;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.Counter;

@RunWith(MockitoJUnitRunner.class)
public class TableValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TableValidator validator = new TableValidator();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cluster cluster;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private TableMetadata tableMetaData;

    @Mock
    private ConfigurationContext configContext;

    private String keyspaceName = "keyspace";

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)

    private EntityMeta meta;

    @Before
    public void setUp() {
        when(tableMetaData.getName()).thenReturn("table");
        when(meta.config().isSchemaUpdateEnabled()).thenReturn(false);
    }

    @Test
    public void should_validate_id_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).cqlColumnName("id").type(PARTITION_KEY).build();
        PropertyMeta nameMeta = completeBean(Void.class, String.class).cqlColumnName("name").type(SIMPLE).build();

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.structure().isCompoundPK()).thenReturn(false);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(nameMeta));
        when(meta.structure().isClusteredCounter()).thenReturn(false);

        ColumnMetadata idMetadata = ColumnMetadataBuilder.create(tableMetaData, "id", bigint());
        when(tableMetaData.getColumn("id")).thenReturn(idMetadata);

        ColumnMetadata nameMetadata = ColumnMetadataBuilder.create(tableMetaData, "name", text());
        when(tableMetaData.getColumn("name")).thenReturn(nameMetadata);

        validator.validateForEntity(meta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_compound_pk_for_entity() throws Exception {
        PropertyMeta userId = valueClass(Long.class).propertyName("userId").cqlColumnName("userid").type(SIMPLE).build();
        PropertyMeta name = valueClass(String.class).propertyName("name").cqlColumnName("name").type(SIMPLE).build();

        PropertyMeta idMeta = valueClass(CompoundPK.class).type(COMPOUND_PRIMARY_KEY)
                .propertyName("compound")
                .partitionKeyMetas(userId).clusteringKeyMetas(name)
                .clusteringOrders(new ClusteringOrder("name", Sorting.ASC))
                .build();

        PropertyMeta stringMeta = valueClass(String.class).propertyName("string").cqlColumnName("string").type(SIMPLE).build();

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.structure().isCompoundPK()).thenReturn(true);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(stringMeta));
        when(meta.structure().isClusteredCounter()).thenReturn(false);

        ColumnMetadata userIdMetadata = ColumnMetadataBuilder.create(tableMetaData, "userid", bigint());
        when(tableMetaData.getColumn("userid")).thenReturn(userIdMetadata);
        when(tableMetaData.getPartitionKey()).thenReturn(asList(userIdMetadata));

        ColumnMetadata nameMetadata = ColumnMetadataBuilder.create(tableMetaData, "name", text());
        when(tableMetaData.getColumn("name")).thenReturn(nameMetadata);
        when(tableMetaData.getClusteringColumns()).thenReturn(asList(nameMetadata));

        ColumnMetadata stringMetadata = ColumnMetadataBuilder.create(tableMetaData, "string", text());
        when(tableMetaData.getColumn("string")).thenReturn(stringMetadata);

        validator.validateForEntity(meta, tableMetaData, configContext);
    }


    @Test
    public void should_validate_simple_field_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).cqlColumnName("id").type(PARTITION_KEY).build();
        PropertyMeta simpleMeta = completeBean(Void.class, String.class).cqlColumnName("name").type(SIMPLE)
                .build();

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.structure().isCompoundPK()).thenReturn(false);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(simpleMeta));
        when(meta.structure().isClusteredCounter()).thenReturn(false);

        ColumnMetadata idMetadata = ColumnMetadataBuilder.create(tableMetaData, "id", bigint());
        when(tableMetaData.getColumn("id")).thenReturn(idMetadata);

        ColumnMetadata simpleMetadata = ColumnMetadataBuilder.create(tableMetaData, "name", text());
        when(tableMetaData.getColumn("name")).thenReturn(simpleMetadata);

        validator.validateForEntity(meta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_collection_and_map_fields_for_entity() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).cqlColumnName("id").type(PARTITION_KEY).build();

        PropertyMeta listMeta = completeBean(Void.class, String.class).cqlColumnName("friends").type(LIST).build();
        PropertyMeta setMeta = completeBean(Void.class, String.class).cqlColumnName("followers").type(SET).build();
        PropertyMeta mapMeta = completeBean(Integer.class, String.class).cqlColumnName("preferences").type(MAP).build();

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.structure().isCompoundPK()).thenReturn(false);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(listMeta, setMeta, mapMeta));
        when(meta.structure().isClusteredCounter()).thenReturn(false);

        ColumnMetadata idMetadata = ColumnMetadataBuilder.create(tableMetaData, "id", bigint());
        when(tableMetaData.getColumn("id")).thenReturn(idMetadata);

        ColumnMetadata friendsMetadata = ColumnMetadataBuilder.create(tableMetaData, "friends", list(text()));
        when(tableMetaData.getColumn("friends")).thenReturn(friendsMetadata);

        ColumnMetadata followersMetadata = ColumnMetadataBuilder.create(tableMetaData, "followers", set(text()));
        when(tableMetaData.getColumn("followers")).thenReturn(followersMetadata);

        ColumnMetadata preferencesMetadata = ColumnMetadataBuilder.create(tableMetaData, "preferences", map(cint(),text()));
        when(tableMetaData.getColumn("preferences")).thenReturn(preferencesMetadata);

        validator.validateForEntity(meta, tableMetaData, configContext);
    }


    @Test
    public void should_validate_clustered_counter_fields() throws Exception {
        //Given
        PropertyMeta userId = valueClass(Long.class).cqlColumnName("userid").type(SIMPLE).build();
        PropertyMeta name = valueClass(String.class).cqlColumnName("name").type(SIMPLE).build();

        PropertyMeta idMeta = valueClass(CompoundPK.class).type(COMPOUND_PRIMARY_KEY)
                .propertyName("compound")
                .partitionKeyMetas(userId).clusteringKeyMetas(name)
                .clusteringOrders(new ClusteringOrder("name", Sorting.ASC))
                .build();

        PropertyMeta counter = completeBean(Void.class, Counter.class).cqlColumnName("count")
                .type(COUNTER).build();

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.structure().isCompoundPK()).thenReturn(true);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(new ArrayList<PropertyMeta>());
        when(meta.getAllCounterMetas()).thenReturn(asList(counter));
        when(meta.structure().isClusteredCounter()).thenReturn(true);

        ColumnMetadata userIdMetadata = ColumnMetadataBuilder.create(tableMetaData, "userid", bigint());
        when(tableMetaData.getColumn("userid")).thenReturn(userIdMetadata);
        when(tableMetaData.getPartitionKey()).thenReturn(asList(userIdMetadata));

        ColumnMetadata nameMetadata = ColumnMetadataBuilder.create(tableMetaData, "name", text());
        when(tableMetaData.getColumn("name")).thenReturn(nameMetadata);
        when(tableMetaData.getClusteringColumns()).thenReturn(asList(nameMetadata));

        ColumnMetadata counterMetadata = ColumnMetadataBuilder.create(tableMetaData, "count", counter());
        when(tableMetaData.getColumn("count")).thenReturn(counterMetadata);

        //When
        validator.validateForEntity(meta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_achilles_counter() throws Exception {
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());
        ColumnMetadata propertyColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PROPERTY_NAME, text());
        ColumnMetadata counterColumnMeta = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_VALUE, counter());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_VALUE)).thenReturn(counterColumnMeta);

        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));
        when(tableMetaData.getClusteringColumns()).thenReturn(asList(propertyColumn));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_table_not_found() throws Exception {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(null);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find table '%s' from keyspace '%s'", ACHILLES_COUNTER_TABLE, keyspaceName));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_fqcn_column() throws Exception {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(null);
        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", ACHILLES_COUNTER_FQCN,ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_fqcn_column_bad_type() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, inet());
        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'", ACHILLES_COUNTER_FQCN, inet(), text()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_not_matching_counter_fqcn_column() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getPartitionKey()).thenReturn(new ArrayList<ColumnMetadata>());

        // Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(String.format("Column '%s' of table '%s' should be a partition key component",ACHILLES_COUNTER_FQCN, ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_pk_column() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(null);
        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn));

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", ACHILLES_COUNTER_PRIMARY_KEY,ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_pk_column_bad_type() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, inet());
        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'",ACHILLES_COUNTER_PRIMARY_KEY, inet(), text()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_pk_column_not_matching() {
        // Given
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());
        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn));

        // Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(String.format("Column '%s' of table '%s' should be a partition key component",ACHILLES_COUNTER_PRIMARY_KEY, ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_property_column() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME)).thenReturn(null);

        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", ACHILLES_COUNTER_PROPERTY_NAME,ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_property_column_bad_type() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());
        ColumnMetadata propertyColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PROPERTY_NAME, inet());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumn);

        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));
        when(tableMetaData.getClusteringColumns()).thenReturn(asList(propertyColumn));

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'",ACHILLES_COUNTER_PROPERTY_NAME, inet(), text()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_property_column_not_matching() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());
        ColumnMetadata propertyColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PROPERTY_NAME, text());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumn);

        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));
        when(tableMetaData.getClusteringColumns()).thenReturn(new ArrayList<ColumnMetadata>());

        // Then
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(String.format("Column '%s' of table '%s' should be a clustering key component",ACHILLES_COUNTER_PROPERTY_NAME, ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_no_counter_value_column() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());
        ColumnMetadata propertyColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PROPERTY_NAME, text());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_VALUE)).thenReturn(null);

        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));
        when(tableMetaData.getClusteringColumns()).thenReturn(asList(propertyColumn));

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Cannot find column '%s' from table '%s'", ACHILLES_COUNTER_VALUE,ACHILLES_COUNTER_TABLE));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_exception_when_counter_value_column_bad_type() {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
        when(keyspaceMeta.getTable(ACHILLES_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_FQCN, text());
        ColumnMetadata pkColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PRIMARY_KEY, text());
        ColumnMetadata propertyColumn = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_PROPERTY_NAME, text());
        ColumnMetadata counterColumnMeta = ColumnMetadataBuilder.create(tableMetaData, ACHILLES_COUNTER_VALUE, inet());

        when(tableMetaData.getColumn(ACHILLES_COUNTER_FQCN)).thenReturn(fqcnColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY)).thenReturn(pkColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumn);
        when(tableMetaData.getColumn(ACHILLES_COUNTER_VALUE)).thenReturn(counterColumnMeta);

        when(tableMetaData.getPartitionKey()).thenReturn(asList(fqcnColumn, pkColumn));
        when(tableMetaData.getClusteringColumns()).thenReturn(asList(propertyColumn));

        // Then
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage(String.format("Column '%s' of type '%s' should be of type '%s'", ACHILLES_COUNTER_VALUE, inet(), counter()));

        validator.validateAchillesCounter(keyspaceMeta, keyspaceName);
    }
}
