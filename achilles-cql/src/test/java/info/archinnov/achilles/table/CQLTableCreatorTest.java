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

import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Counter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

@RunWith(MockitoJUnitRunner.class)
public class CQLTableCreatorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLTableCreator creator;

	@Mock
	private Session session;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private Cluster cluster;

	@Mock
	private KeyspaceMetadata keyspaceMeta;

	@Mock
	private CQLTableValidator validator;

	@Captor
	private ArgumentCaptor<String> stringCaptor;

	private String keyspaceName = "achilles";

	private Map<String, TableMetadata> tableMetas;

	private EntityMeta meta;

	@Before
	public void setUp() {
		tableMetas = new LinkedHashMap<String, TableMetadata>();
		when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
		when(keyspaceMeta.getTables()).thenReturn(new ArrayList<TableMetadata>());

		creator = new CQLTableCreator(cluster, session, keyspaceName);
		Whitebox.setInternalState(creator, Map.class, tableMetas);
		Whitebox.setInternalState(creator, CQLTableValidator.class, validator);
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
		meta.setAllMetasExceptIdMeta(Arrays.asList(longColPM, longListColPM, longSetColPM, longMapColPM));
		meta.setIdMeta(idMeta);
		meta.setTableName("tableName");
		meta.setClassName("entityName");

		creator.validateOrCreateTableForEntity(meta, true);

		verify(session).execute(stringCaptor.capture());

		assertThat(stringCaptor.getValue()).isEqualTo(
				"\n\tCREATE TABLE tableName(\n" + "\t\tlongCol bigint,\n" + "\t\tid bigint,\n"
						+ "\t\tlongListCol list<bigint>,\n" + "\t\tlongSetCol set<bigint>,\n"
						+ "\t\tlongMapCol map<int,bigint>,\n" + "\t\tPRIMARY KEY(id)\n"
						+ "\t) WITH COMMENT = 'Create table for entity \"entityName\"'");
	}

	@Test
	public void should_create_indices_scripts() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).field("id").build();

		PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();
		longColPM.setIndexed(true);

		meta = new EntityMeta();
		meta.setAllMetasExceptIdMeta(Arrays.asList(longColPM));
		meta.setIdMeta(idMeta);
		meta.setTableName("tableName");
		meta.setClassName("entityName");

		creator.validateOrCreateTableForEntity(meta, true);

		verify(session, new Times(2)).execute(stringCaptor.capture());

		assertThat(stringCaptor.getValue()).isEqualTo(
				"\nCREATE INDEX tableName_longCol\n" + "ON tableName (longCol);\n");
	}

	@Test
	public void should_create_clustered_table() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).field("id")
				.compNames("index", "count", "uuid").compClasses(Long.class, Integer.class, UUID.class)
				.compTimeUUID("uuid").build();

		PropertyMeta longColPM = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).field("longCol").build();

		meta = new EntityMeta();
		meta.setAllMetasExceptIdMeta(Arrays.asList(longColPM));
		meta.setIdMeta(idMeta);
		meta.setTableName("tableName");
		meta.setClassName("entityName");

		creator.validateOrCreateTableForEntity(meta, true);

		verify(session).execute(stringCaptor.capture());

		assertThat(stringCaptor.getValue()).isEqualTo(
				"\n\tCREATE TABLE tableName(\n" + "\t\tlongCol bigint,\n" + "\t\tindex bigint,\n" + "\t\tcount int,\n"
						+ "\t\tuuid timeuuid,\n" + "\t\tPRIMARY KEY(index, count, uuid)\n"
						+ "\t) WITH COMMENT = 'Create table for entity \"entityName\"'");

	}

	@Test
	public void should_create_clustered_counter_table() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).field("id")
				.compNames("index", "count", "uuid").compClasses(Long.class, Integer.class, UUID.class).build();

		PropertyMeta counterColPM = PropertyMetaTestBuilder.keyValueClass(Void.class, Counter.class).type(COUNTER)
				.field("counterCol").build();

		meta = new EntityMeta();
		meta.setFirstMeta(counterColPM);
		meta.setIdMeta(idMeta);
		meta.setClusteredCounter(true);
		meta.setTableName("tableName");
		meta.setClassName("entityName");

		creator.validateOrCreateTableForEntity(meta, true);

		verify(session).execute(stringCaptor.capture());

		assertThat(stringCaptor.getValue()).isEqualTo(
				"\n\tCREATE TABLE tableName(\n" + "\t\tindex bigint,\n" + "\t\tcount int,\n" + "\t\tuuid uuid,\n"
						+ "\t\tcounterCol counter,\n" + "\t\tPRIMARY KEY(index, count, uuid)\n"
						+ "\t) WITH COMMENT = 'Create table for clustered counter entity \"entityName\"'");

	}

	@Test
	public void should_validate_table_when_already_exists() throws Exception {
		TableMetadata tableMetadata = mock(TableMetadata.class);
		tableMetas.put("tablename", tableMetadata);

		meta = new EntityMeta();
		meta.setTableName("tableName");
		meta.setClassName("entityName");

		creator.validateOrCreateTableForEntity(meta, true);

		verify(validator).validateForEntity(meta, tableMetadata);
		verifyZeroInteractions(session);
	}

	@Test
	public void should_exception_when_table_does_not_exist() throws Exception {
		meta = new EntityMeta();
		meta.setTableName("tableName");
		meta.setClassName("entityName");

		exception.expect(AchillesInvalidTableException.class);
		exception.expectMessage("The required table 'tablename' does not exist for entity 'entityName'");

		creator.validateOrCreateTableForEntity(meta, false);
	}

	@Test
	public void should_create_achilles_counter_table() throws Exception {
		creator.validateOrCreateTableForCounter(true);

		verify(session).execute(stringCaptor.capture());

		assertThat(stringCaptor.getValue()).isEqualTo(
				"\n\tCREATE TABLE " + CQL_COUNTER_TABLE + "(\n" + "\t\t" + CQL_COUNTER_FQCN + " text,\n" + "\t\t"
						+ CQL_COUNTER_PRIMARY_KEY + " text,\n" + "\t\t" + CQL_COUNTER_PROPERTY_NAME + " text,\n"
						+ "\t\t" + CQL_COUNTER_VALUE + " counter,\n" + "\t\tPRIMARY KEY((" + CQL_COUNTER_FQCN + ", "
						+ CQL_COUNTER_PRIMARY_KEY + "), " + CQL_COUNTER_PROPERTY_NAME + ")\n"
						+ "\t) WITH COMMENT = 'Create default Achilles counter table \"" + CQL_COUNTER_TABLE + "\"'");
	}

	@Test
	public void should_validate_achilles_counter_table_when_already_exist() throws Exception {
		TableMetadata tableMetadata = mock(TableMetadata.class);
		tableMetas.put(CQL_COUNTER_TABLE, tableMetadata);

		creator.validateOrCreateTableForCounter(false);

		verify(validator).validateAchillesCounter();
		verifyZeroInteractions(session);
	}

	@Test
	public void should_exception_when_achilles_counter_table_does_not_exist() throws Exception {

		exception.expect(AchillesInvalidTableException.class);
		exception.expectMessage("The required generic table '" + CQL_COUNTER_TABLE + "' does not exist");

		creator.validateOrCreateTableForCounter(false);
	}
}
