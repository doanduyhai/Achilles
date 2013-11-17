/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.table;

import static com.datastax.driver.core.ColumnMetadata.IndexMetadata;
import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.IndexProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class CQLTableValidatorTest {

	private CQLTableValidator validator;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private Cluster cluster;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private TableMetadata tableMetaData;

    @Mock
    private CQLColumnMetaDataComparator columnMetaDataComparator;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private ColumnMetadata columnMetadata;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private ColumnMetadata columnMetadataForField;

	private String keyspaceName = "keyspace";

	private EntityMeta entityMeta;

	@Before
	public void setUp() {
		validator = new CQLTableValidator(cluster, keyspaceName);
        Whitebox.setInternalState(validator,"columnMetaDataComparator",columnMetaDataComparator);
		entityMeta = new EntityMeta();
        when(columnMetadata.getIndex()).thenReturn(null);
        when(columnMetadataForField.getIndex()).thenReturn(null);
	}

	@Test
	public void should_validate_id_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(nameMeta));

		when(tableMetaData.getName()).thenReturn("table");
		when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
		when(columnMetadata.getType()).thenReturn(DataType.bigint());

		when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.text());

		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_embedded_id_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compNames("userId", "name")
				.compClasses(Long.class, String.class).type(EMBEDDED_ID).build();


		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("string")
				.type(SIMPLE).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(nameMeta));

        ColumnMetadata userColumn = mock(ColumnMetadata.class);
        ColumnMetadata nameColumn = mock(ColumnMetadata.class);

		when(tableMetaData.getName()).thenReturn("table");
		ColumnMetadata userIdMetadata = mock(ColumnMetadata.class);
		when(tableMetaData.getColumn("userid")).thenReturn(userIdMetadata);
		when(userIdMetadata.getType()).thenReturn(DataType.bigint());
		when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(userColumn));
        when(columnMetaDataComparator.isEqual(userIdMetadata, userColumn)).thenReturn(true);

        ColumnMetadata nameMetadata = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn("name")).thenReturn(nameMetadata);
        when(nameMetadata.getType()).thenReturn(DataType.text());
        when(tableMetaData.getClusteringKey()).thenReturn(Arrays.asList(nameColumn));
        when(columnMetaDataComparator.isEqual(nameMetadata, nameColumn)).thenReturn(true);

        when(tableMetaData.getColumn("string")).thenReturn(columnMetadataForField);
        when(columnMetadataForField.getType()).thenReturn(DataType.text());

		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_embedded_id_with_time_uuid_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compNames("userId", "date")
				.compClasses(Long.class, UUID.class).type(EMBEDDED_ID).compTimeUUID("date").build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("string")
				.type(SIMPLE).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(nameMeta));

        ColumnMetadata userColumn = mock(ColumnMetadata.class);
        ColumnMetadata nameColumn = mock(ColumnMetadata.class);

        when(tableMetaData.getName()).thenReturn("table");
		ColumnMetadata userIdMetadata = mock(ColumnMetadata.class);
		when(tableMetaData.getColumn("userid")).thenReturn(userIdMetadata);
		when(userIdMetadata.getType()).thenReturn(DataType.bigint());
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(userColumn));
        when(columnMetaDataComparator.isEqual(userIdMetadata, userColumn)).thenReturn(true);


        ColumnMetadata nameMetadata = mock(ColumnMetadata.class);
		when(tableMetaData.getColumn("date")).thenReturn(nameMetadata);
		when(nameMetadata.getType()).thenReturn(DataType.timeuuid());
        when(tableMetaData.getClusteringKey()).thenReturn(Arrays.asList(nameColumn));
        when(columnMetaDataComparator.isEqual(nameMetadata, nameColumn)).thenReturn(true);

		when(tableMetaData.getColumn("string")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.text());

		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_simple_field_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").type(SIMPLE)
				.build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(tableMetaData.getName()).thenReturn("table");
		when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
		when(columnMetadata.getType()).thenReturn(DataType.bigint());
		when(columnMetadata.getIndex()).thenReturn(null);
		when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.text());
		when(columnMetadataForField.getIndex()).thenReturn(null);
		validator.validateForEntity(entityMeta, tableMetaData);

		pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").type(LAZY_SIMPLE).build();
		entityMeta.setPropertyMetas(ImmutableMap.of("name", pm));
		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_simple_indexed_field_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").type(SIMPLE)
				.build();
		pm.setIndexProperties(new IndexProperties(""));

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(tableMetaData.getName()).thenReturn("table");
		when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
		when(columnMetadata.getType()).thenReturn(DataType.bigint());
		when(columnMetadata.getIndex()).thenReturn(null);
		when(tableMetaData.getColumn("name")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.text());

		IndexMetadata indexMetadata = mock(IndexMetadata.class);
		when(indexMetadata.getName()).thenReturn("table(name)");
		when(indexMetadata.getIndexedColumn()).thenReturn(columnMetadataForField);

		when(columnMetadataForField.getIndex()).thenReturn(indexMetadata);
		validator.validateForEntity(entityMeta, tableMetaData);

	}

@Test
	public void should_validate_list_field_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends").type(LIST)
				.build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(tableMetaData.getName()).thenReturn("table");
		when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
		when(columnMetadata.getType()).thenReturn(DataType.bigint());

		when(tableMetaData.getColumn("friends")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.list(DataType.text()));

		validator.validateForEntity(entityMeta, tableMetaData);

		pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends").type(LAZY_LIST).build();
		entityMeta.setPropertyMetas(ImmutableMap.of("friends", pm));
		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_set_field_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("followers").type(SET)
				.build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(tableMetaData.getName()).thenReturn("table");
		when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
		when(columnMetadata.getType()).thenReturn(DataType.bigint());

		when(tableMetaData.getColumn("followers")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.set(DataType.text()));

		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_map_field_for_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Integer.class, String.class).field("preferences")
				.type(MAP).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(tableMetaData.getName()).thenReturn("table");
		when(tableMetaData.getColumn("id")).thenReturn(columnMetadata);
		when(columnMetadata.getType()).thenReturn(DataType.bigint());

		when(tableMetaData.getColumn("preferences")).thenReturn(columnMetadataForField);
		when(columnMetadataForField.getType()).thenReturn(DataType.map(DataType.cint(), DataType.text()));

		validator.validateForEntity(entityMeta, tableMetaData);

		pm = PropertyMetaTestBuilder.completeBean(Integer.class, String.class).field("preferences").type(LAZY_MAP)
				.build();
		entityMeta.setPropertyMetas(ImmutableMap.of("preferences", pm));
		validator.validateForEntity(entityMeta, tableMetaData);
	}

	@Test
	public void should_validate_achilles_counter() throws Exception {
		KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);
		when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
		when(keyspaceMeta.getTable(CQL_COUNTER_TABLE)).thenReturn(tableMetaData);

        ColumnMetadata fqcnColumn = mock(ColumnMetadata.class);
        ColumnMetadata pkColumn = mock(ColumnMetadata.class);
        ColumnMetadata propertyColumn = mock(ColumnMetadata.class);
        when(tableMetaData.getPartitionKey()).thenReturn(Arrays.asList(fqcnColumn,pkColumn));

        ColumnMetadata fqcnColumnMeta = mock(ColumnMetadata.class);
        when(tableMetaData.getColumn(CQL_COUNTER_FQCN)).thenReturn(fqcnColumnMeta);
        when(columnMetaDataComparator.isEqual(fqcnColumnMeta, fqcnColumn)).thenReturn(true);

		ColumnMetadata pkColumnMeta = mock(ColumnMetadata.class);
		when(tableMetaData.getColumn(CQL_COUNTER_PRIMARY_KEY)).thenReturn(pkColumnMeta);
        when(columnMetaDataComparator.isEqual(pkColumnMeta, pkColumn)).thenReturn(true);

		ColumnMetadata propertyColumnMeta = mock(ColumnMetadata.class);
		when(tableMetaData.getColumn(CQL_COUNTER_PROPERTY_NAME)).thenReturn(propertyColumnMeta);
        when(tableMetaData.getClusteringKey()).thenReturn(Arrays.asList(propertyColumn));
        when(columnMetaDataComparator.isEqual(propertyColumnMeta, propertyColumn)).thenReturn(true);

		ColumnMetadata counterColumnMeta = mock(ColumnMetadata.class);
		when(tableMetaData.getColumn(CQL_COUNTER_VALUE)).thenReturn(counterColumnMeta);

		when(fqcnColumnMeta.getType()).thenReturn(DataType.text());
		when(pkColumnMeta.getType()).thenReturn(DataType.text());
		when(propertyColumnMeta.getType()).thenReturn(DataType.text());
		when(counterColumnMeta.getType()).thenReturn(DataType.counter());

		validator.validateAchillesCounter();

	}
}
