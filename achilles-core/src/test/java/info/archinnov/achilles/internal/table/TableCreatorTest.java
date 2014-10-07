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

import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.metadata.holder.*;
import info.archinnov.achilles.json.DefaultJacksonMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
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

    @Mock
    private ConfigurationContext configContext;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    private ObjectMapper defaultJakcsonMapper = new DefaultJacksonMapperFactory().getMapper(String.class);

    private String keyspaceName = "achilles";


    @Before
    public void setUp() {
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        when(keyspaceMeta.getTables()).thenReturn(new ArrayList<TableMetadata>());
        when(configContext.isForceColumnFamilyCreation()).thenReturn(true);
        when(meta.config().getQualifiedTableName()).thenReturn("myTable");
        when(meta.config().getTableName()).thenReturn("myTable");
        when(meta.config().getTableComment()).thenReturn("test table");
        when(meta.getClassName()).thenReturn("CompleteBean");
        creator = new TableCreator();

    }

    @Test
    public void should_create_complete_table() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).field("id").build();

        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").indexed("simple_idx").build();

        PropertyMeta longListColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(LIST).field("longListCol").build();

        PropertyMeta longSetColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SET).field("longSetCol").build();

        PropertyMeta longMapColPM = PropertyMetaTestBuilder.keyValueClass(Integer.class, Long.class).type(MAP).field("longMapCol").build();

        when(meta.structure().isClusteredCounter()).thenReturn(false);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(longColPM, longListColPM, longSetColPM, longMapColPM));
        when(meta.getIdMeta()).thenReturn(idMeta);

        creator.createTableForEntity(session, meta, configContext);

        verify(session, times(2)).execute(stringCaptor.capture());

        final List<String> scripts = stringCaptor.getAllValues();

        assertThat(scripts).hasSize(2);
        assertThat(scripts.get(0)).isEqualTo(
                "\n\tCREATE TABLE myTable(\n"
                        + "\t\tid bigint,\n"
                        + "\t\tlongcol bigint,\n"
                        + "\t\tlonglistcol list<bigint>,\n"
                        + "\t\tlongsetcol set<bigint>,\n"
                        + "\t\tlongmapcol map<int,bigint>,\n"
                        + "\t\tPRIMARY KEY(id))\n"
                        + "\tWITH comment = 'test table'");

        assertThat(scripts.get(1)).isEqualTo("\n\tCREATE INDEX simple_idx ON myTable(longcol)");
    }

    @Test
    public void should_create_complete_table_with_clustering_order() throws Exception {

        PropertyMeta idPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("id").build();
        PropertyMeta namePM = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).field("name").build();
        PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();

        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
                .type(EMBEDDED_ID).field("compound")
                .partitionKeyMetas(idPM).clusteringKeyMetas(namePM)
                .clusteringOrders(new ClusteringOrder("name", Sorting.DESC))
                .build();

        when(meta.structure().isClusteredCounter()).thenReturn(false);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(longColPM));
        when(meta.getIdMeta()).thenReturn(idMeta);

        creator.createTableForEntity(session, meta, configContext);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE myTable(\n"
                        + "\t\tid bigint,\n"
                        + "\t\tname text,\n"
                        + "\t\tlongcol bigint,\n"
                        + "\t\tPRIMARY KEY(id, name))\n"
                        + "\tWITH comment = 'test table'"
                        + " AND CLUSTERING ORDER BY(name DESC)");
    }


    @Test
    public void should_create_clustered_counter_table() throws Exception {
        PropertyMeta idPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("id").build();
        PropertyMeta namePM = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).field("name").build();

        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
                .type(EMBEDDED_ID).field("compound")
                .partitionKeyMetas(idPM).clusteringKeyMetas(namePM)
                .clusteringOrders(new ClusteringOrder("name", Sorting.DESC))
                .build();
        PropertyMeta counterColPM = PropertyMetaTestBuilder.keyValueClass(Void.class, Counter.class).type(COUNTER).field("counterCol").build();

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.structure().isClusteredCounter()).thenReturn(true);
        when(meta.getAllCounterMetas()).thenReturn(asList(counterColPM));

        creator.createTableForEntity(session, meta, configContext);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE myTable(\n"
                        + "\t\tid bigint,\n"
                        + "\t\tname text,\n"
                        + "\t\tcountercol counter,\n"
                        + "\t\tPRIMARY KEY(id, name))\n"
                        + "\tWITH comment = 'test table'"
                        + " AND CLUSTERING ORDER BY(name DESC)");

    }

    @Test
    public void should_exception_when_table_does_not_exist() throws Exception {
        when(configContext.isForceColumnFamilyCreation()).thenReturn(false);

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The required table 'myTable' does not exist for entity 'CompleteBean'");

        creator.createTableForEntity(session, meta, configContext);
    }

    @Test
    public void should_create_achilles_counter_table() throws Exception {
        creator.createTableForCounter(session, configContext);

        verify(session).execute(stringCaptor.capture());

        assertThat(stringCaptor.getValue()).isEqualTo(
                "\n\tCREATE TABLE " + ACHILLES_COUNTER_TABLE + "(\n" + "\t\t" + ACHILLES_COUNTER_FQCN + " text,\n" + "\t\t"
                        + ACHILLES_COUNTER_PRIMARY_KEY + " text,\n" + "\t\t" + ACHILLES_COUNTER_PROPERTY_NAME + " text,\n"
                        + "\t\t" + ACHILLES_COUNTER_VALUE + " counter,\n" + "\t\tPRIMARY KEY((" + ACHILLES_COUNTER_FQCN + ", "
                        + ACHILLES_COUNTER_PRIMARY_KEY + "), "
                        + ACHILLES_COUNTER_PROPERTY_NAME + "))\n"
                        + "\tWITH comment = 'Create default Achilles counter table \"" + ACHILLES_COUNTER_TABLE + "\"'");
    }

    @Test
    public void should_exception_when_achilles_counter_table_does_not_exist() throws Exception {

        when(configContext.isForceColumnFamilyCreation()).thenReturn(false);
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The required generic table '" + ACHILLES_COUNTER_TABLE + "' does not exist");

        creator.createTableForCounter(session, configContext);
    }
}
