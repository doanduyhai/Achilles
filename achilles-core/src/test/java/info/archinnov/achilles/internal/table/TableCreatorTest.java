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

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.internal.verification.Times;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.metadata.holder.ClusteringComponents;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdProperties;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PartitionComponents;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.Bean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Counter;

@RunWith(MockitoJUnitRunner.class)
public class TableCreatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TableCreator creator;

    @Mock
    private Session session;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cluster cluster;

    @Mock
    private KeyspaceMetadata keyspaceMeta;

    @Mock
    private TableMetadata tableMeta;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    private String keyspaceName = "achilles";

    private EntityMeta meta;

    @Before
    public void setUp() {
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTables()).thenReturn(new ArrayList<TableMetadata>());
        creator = new TableCreator();
    }

    @Test
    public void should_fetch_table_metas() throws Exception {
        // Given
        List<TableMetadata> tableMetas = asList(tableMeta);

        // When
        when(keyspaceMeta.getTables()).thenReturn(tableMetas);
        when(tableMeta.getName()).thenReturn("table");

        Map<String, TableMetadata> actual = creator.fetchTableMetaData(keyspaceMeta, "keyspace");

        // Then
        assertThat(actual.get("table")).isSameAs(tableMeta);
    }

    @Test
    public void should_create_complete_table() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).field("id").build();

        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();

        PropertyMeta longListColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(LIST).field("longListCol")
                .build();

        PropertyMeta longSetColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SET).field("longSetCol")
                .build();

        PropertyMeta longMapColPM = PropertyMetaTestBuilder.keyValueClass(Integer.class, Long.class).type(MAP)
                .field("longMapCol").build();

        meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(asList(longColPM, longListColPM, longSetColPM, longMapColPM));
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        creator.createTableForEntity(session, meta, true);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE tableName(\n"
                        + "\t\tid bigint,\n"
                        + "\t\tlongCol bigint,\n"
                        + "\t\tlongListCol list<bigint>,\n" + "\t\tlongSetCol set<bigint>,\n"
                        + "\t\tlongMapCol map<int,bigint>,\n" + "\t\tPRIMARY KEY(id))\n"
                        + "\tWITH comment = 'Create table for entity \"entityName\"'");
    }

    @Test
    public void should_create_complete_table_with_clustering_order() throws Exception {
        PropertyMeta idMeta = new PropertyMeta();
        idMeta.setType(PropertyType.EMBEDDED_ID);
        PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>>asList(Long.class),asList("id"), new ArrayList<Field>(), new ArrayList<Method>(), new ArrayList<Method>());
        ClusteringOrder clusteringOrder = new ClusteringOrder("name", Sorting.DESC);
        ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>>asList(String.class),asList("name"), null, null, null,Arrays.asList(clusteringOrder));
        EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents,
                new ArrayList<Class<?>>(), asList("a", "b", "c"), new ArrayList<Field>(), new ArrayList<Method>(),
                new ArrayList<Method>(), new ArrayList<String>());
        idMeta.setEmbeddedIdProperties(props);

        Map<String, PropertyMeta> propertyMetas = new HashMap<>();
        PropertyMeta simpleMeta = new PropertyMeta();
        simpleMeta.setType(SIMPLE);
        Method getter = Bean.class.getDeclaredMethod("getName", (Class<?>[]) null);
        simpleMeta.setGetter(getter);
        Method setter = Bean.class.getDeclaredMethod("setName", String.class);
        simpleMeta.setSetter(setter);
        propertyMetas.put("name", simpleMeta);

        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();

        meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(asList(longColPM));
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        creator.createTableForEntity(session, meta, true);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE tableName(\n"
                        + "\t\tid bigint,\n"
                        + "\t\tname text,\n"
                        + "\t\tlongCol bigint,\n"
                        + "\t\tPRIMARY KEY(id, name))\n"
                        + "\tWITH comment = 'Create table for entity \"entityName\"'"
                        + " AND CLUSTERING ORDER BY(name DESC)");
    }

    @Test
    public void should_create_indices_scripts() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).field("id").build();

        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();
        longColPM.setIndexProperties(new IndexProperties("", ""));

        meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(asList(longColPM));
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        creator.createTableForEntity(session, meta, true);

        verify(session, new Times(2)).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo("\n\tCREATE INDEX tableName_longCol ON tableName(longCol)");

    }

    @Test
    public void should_create_indices_scripts_with_custom_name() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).field("id").build();

        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();
        longColPM.setIndexProperties(new IndexProperties("myIndex", "longCol"));

        meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(asList(longColPM));
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        creator.createTableForEntity(session, meta, true);

        verify(session, new Times(2)).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo("\n\tCREATE INDEX myIndex ON tableName(longCol)");
    }

    @Test
    public void should_create_clustered_table() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).field("id")
                .compNames("indexCol", "count", "uuid").compClasses(Long.class, Integer.class, UUID.class)
                .compTimeUUID("uuid").clusteringOrders(new ClusteringOrder("count", Sorting.DESC)).build();

        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();
        longColPM.setStaticColumn(true);

        meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(asList(longColPM));
        meta.setIdMeta(idMeta);
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        creator.createTableForEntity(session, meta, true);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE tableName(\n"
                        + "\t\tindexCol bigint,\n"
                        + "\t\tcount int,\n"
                        + "\t\tuuid timeuuid,\n"
                        + "\t\tlongCol bigint static,\n"
                        + "\t\tPRIMARY KEY(indexCol, count, uuid))\n"
                        + "\tWITH comment = 'Create table for entity \"entityName\"' AND CLUSTERING ORDER BY(count DESC)");

    }

    @Test
    public void should_create_clustered_counter_table() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).field("id")
                .compNames("indexCol", "count", "uuid").compClasses(Long.class, Integer.class, UUID.class)
                .clusteringOrders(new ClusteringOrder("count", Sorting.DESC)).build();

        PropertyMeta counterColPM = PropertyMetaTestBuilder.keyValueClass(Void.class, Counter.class).type(COUNTER)
                .field("counterCol").build();
        counterColPM.setStaticColumn(true);

        meta = new EntityMeta();
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "counter", counterColPM));
        meta.setAllMetasExceptCounters(asList(idMeta));
        meta.setAllMetasExceptIdAndCounters(asList(counterColPM));
        meta.setIdMeta(idMeta);
        meta.setClusteredCounter(true);
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        creator.createTableForEntity(session, meta, true);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE tableName(\n"
                        + "\t\tindexCol bigint,\n"
                        + "\t\tcount int,\n"
                        + "\t\tuuid uuid,\n"
                        + "\t\tcounterCol counter static,\n"
                        + "\t\tPRIMARY KEY(indexCol, count, uuid))\n"
                        + "\tWITH comment = 'Create table for clustered counter entity \"entityName\"' AND CLUSTERING ORDER BY(count DESC)");

    }

    @Test
    public void should_exception_when_table_does_not_exist() throws Exception {
        meta = new EntityMeta();
        meta.setTableName("tableName");
        meta.setClassName("entityName");

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The required table 'tablename' does not exist for entity 'entityName'");

        creator.createTableForEntity(session, meta, false);
    }

    @Test
    public void should_create_achilles_counter_table() throws Exception {
        creator.createTableForCounter(session, true);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE " + CQL_COUNTER_TABLE + "(\n" + "\t\t" + CQL_COUNTER_FQCN + " text,\n" + "\t\t"
                        + CQL_COUNTER_PRIMARY_KEY + " text,\n" + "\t\t" + CQL_COUNTER_PROPERTY_NAME + " text,\n"
                        + "\t\t" + CQL_COUNTER_VALUE + " counter,\n" + "\t\tPRIMARY KEY((" + CQL_COUNTER_FQCN + ", "
                        + CQL_COUNTER_PRIMARY_KEY + "), "
                        + CQL_COUNTER_PROPERTY_NAME + "))\n"
                        + "\tWITH comment = 'Create default Achilles counter table \"" + CQL_COUNTER_TABLE + "\"'");
    }

    @Test
    public void should_exception_when_achilles_counter_table_does_not_exist() throws Exception {

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The required generic table '" + CQL_COUNTER_TABLE + "' does not exist");

        creator.createTableForCounter(session, false);
    }
}
