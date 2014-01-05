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
package info.archinnov.achilles.internal.statement.prepared;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.*;
import static info.archinnov.achilles.test.builders.PropertyMetaTestBuilder.completeBean;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class PreparedStatementGeneratorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private PreparedStatementGenerator generator = new PreparedStatementGenerator();

	@Mock
	private Session session;

	@Mock
	private PreparedStatement ps;

	@Mock
	private PreparedStatement ps2;

	@Captor
	ArgumentCaptor<String> queryCaptor;

    @Captor
	ArgumentCaptor<RegularStatement> regularStatementCaptor;

	@Test
	public void should_prepare_insert_ps() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta counterMeta = completeBean(Void.class, String.class).field("count")
				.type(PropertyType.COUNTER).build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setTableName("table");
		meta.setAllMetasExceptIdMeta(Arrays.asList(nameMeta, counterMeta));
		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareInsertPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("INSERT INTO table(id,name) VALUES (?,?) USING TTL ?;");
	}

	@Test
	public void should_prepare_insert_ps_with_clustered_id() throws Exception {
		List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.compNames("id", "a", "b").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setTableName("table");
		meta.setAllMetasExceptIdMeta(Arrays.asList(nameMeta));
		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareInsertPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("INSERT INTO table(id,a,b,name) VALUES (?,?,?,?) USING TTL ?;");
	}

	@Test
	public void should_prepare_select_field_ps() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareSelectFieldPS(session, meta, nameMeta);

		assertThat(actual).isSameAs(ps);

		assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM table WHERE id=?;");
	}

	@Test
	public void should_prepare_select_field_ps_for_clustered_id() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.EMBEDDED_ID).compNames("id", "a", "b").build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareSelectFieldPS(session, meta, idMeta);

		assertThat(actual).isSameAs(ps);

		assertThat(queryCaptor.getValue()).isEqualTo("SELECT id,a,b FROM table WHERE id=? AND a=? AND b=?;");
	}

	@Test
	public void should_prepare_update_fields_ps() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta ageMeta = completeBean(Void.class, String.class).field("age")
				.type(PropertyType.SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareUpdateFields(session, meta, Arrays.asList(nameMeta, ageMeta));

		assertThat(actual).isSameAs(ps);

		assertThat(queryCaptor.getValue()).isEqualTo("UPDATE table USING TTL ? SET name=?,age=? WHERE id=?;");
	}

	@Test
	public void should_prepare_update_fields_with_clustered_id_ps() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.compNames("id", "a", "b").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta ageMeta = completeBean(Void.class, String.class).field("age")
				.type(PropertyType.SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareUpdateFields(session, meta, Arrays.asList(nameMeta, ageMeta));

		assertThat(actual).isSameAs(ps);

		assertThat(queryCaptor.getValue()).isEqualTo("UPDATE table USING TTL ? SET name=?,age=? WHERE id=? AND a=? AND b=?;");
	}

	@Test
	public void should_exception_when_preparing_select_for_counter_type() throws Exception {

		PropertyMeta nameMeta = completeBean(Void.class, Long.class).field("count")
				.type(PropertyType.COUNTER).build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("entity");

		exception.expect(IllegalArgumentException.class);
		exception
				.expectMessage("Cannot prepare statement for property 'count' of entity 'entity' because it is a counter type");

		generator.prepareSelectFieldPS(session, meta, nameMeta);

	}

	@Test
	public void should_prepare_select_eager_ps_with_single_key() throws Exception {
		List<PropertyMeta> eagerMetas = new ArrayList<PropertyMeta>();

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		eagerMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setEagerMetas(eagerMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareSelectEagerPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM table WHERE id=?;");
	}

	@Test
	public void should_prepare_select_eager_ps_with_clustered_key() throws Exception {
		List<PropertyMeta> eagerMetas = new ArrayList<PropertyMeta>();

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.compNames("id", "a", "b").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		eagerMetas.add(idMeta);
		eagerMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setEagerMetas(eagerMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.prepareSelectEagerPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("SELECT id,a,b,name FROM table WHERE id=? AND a=? AND b=?;");
	}

	@Test
	public void should_remove_entity_having_single_key() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "name", nameMeta));

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(1);
		assertThat(actual).containsValue(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("DELETE  FROM table WHERE id=?;");
	}

	@Test
	public void should_remove_entity_having_clustered_key() throws Exception {

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.compNames("id", "a", "b").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("name", nameMeta));
		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(1);
		assertThat(actual).containsValue(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("DELETE  FROM table WHERE id=? AND a=? AND b=?;");
	}

	@Test
	public void should_remove_entity_having_counter() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();

		PropertyMeta nameMeta = completeBean(UUID.class, String.class).field("count")
				.type(PropertyType.COUNTER).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("name", nameMeta));

		when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

		Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(1);
		assertThat(actual).containsKey("table");
		assertThat(actual).containsValue(ps);
		assertThat(queryCaptor.getAllValues()).containsOnly("DELETE  FROM table WHERE id=?;");
	}

	@Test
	public void should_prepare_simple_counter_queries() throws Exception {
		PreparedStatement incrPs = mock(PreparedStatement.class);
		PreparedStatement decrPs = mock(PreparedStatement.class);
		PreparedStatement selectPs = mock(PreparedStatement.class);
		PreparedStatement deletePs = mock(PreparedStatement.class);

		when(session.prepare(queryCaptor.capture())).thenReturn(incrPs, decrPs, selectPs, deletePs);

		Map<CQLQueryType, PreparedStatement> actual = generator.prepareSimpleCounterQueryMap(session);

		assertThat(actual.get(INCR)).isSameAs(incrPs);
		assertThat(actual.get(DECR)).isSameAs(decrPs);
		assertThat(actual.get(SELECT)).isSameAs(selectPs);
		assertThat(actual.get(DELETE)).isSameAs(deletePs);

		List<String> queries = queryCaptor.getAllValues();

		assertThat(queries).hasSize(4);
		assertThat(queries.get(0)).isEqualTo(
				"UPDATE " + CQL_COUNTER_TABLE + " SET " + CQL_COUNTER_VALUE + " = " + CQL_COUNTER_VALUE + " + ? WHERE "
						+ CQL_COUNTER_FQCN + " = ? AND " + CQL_COUNTER_PRIMARY_KEY + " = ? AND "
						+ CQL_COUNTER_PROPERTY_NAME + " = ?");
		assertThat(queries.get(1)).isEqualTo(
				"UPDATE " + CQL_COUNTER_TABLE + " SET " + CQL_COUNTER_VALUE + " = " + CQL_COUNTER_VALUE + " - ? WHERE "
						+ CQL_COUNTER_FQCN + " = ? AND " + CQL_COUNTER_PRIMARY_KEY + " = ? AND "
						+ CQL_COUNTER_PROPERTY_NAME + " = ?");
		assertThat(queries.get(2)).isEqualTo(
				"SELECT " + CQL_COUNTER_VALUE + " FROM " + CQL_COUNTER_TABLE + " WHERE " + CQL_COUNTER_FQCN
						+ " = ? AND " + CQL_COUNTER_PRIMARY_KEY + " = ? AND " + CQL_COUNTER_PROPERTY_NAME + " = ?");
		assertThat(queries.get(3)).isEqualTo(
				"DELETE FROM " + CQL_COUNTER_TABLE + " WHERE " + CQL_COUNTER_FQCN + " = ? AND "
						+ CQL_COUNTER_PRIMARY_KEY + " = ? AND " + CQL_COUNTER_PROPERTY_NAME + " = ?");

	}

	@Test
	public void should_prepare_clustered_counter_queries() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

		PropertyMeta counterMeta = completeBean(Void.class, String.class).field("count")
				.type(COUNTER).build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setTableName("counterTable");
		meta.setFirstMeta(counterMeta);

		PreparedStatement incrPs = mock(PreparedStatement.class);
		PreparedStatement decrPs = mock(PreparedStatement.class);
		PreparedStatement selectPs = mock(PreparedStatement.class);
		PreparedStatement deletePs = mock(PreparedStatement.class);

		when(session.prepare(regularStatementCaptor.capture())).thenReturn(incrPs, decrPs, selectPs, deletePs);

		Map<CQLQueryType, PreparedStatement> actual = generator.prepareClusteredCounterQueryMap(session, meta);

		assertThat(actual.get(INCR)).isSameAs(incrPs);
		assertThat(actual.get(DECR)).isSameAs(decrPs);
		assertThat(actual.get(SELECT)).isSameAs(selectPs);
		assertThat(actual.get(DELETE)).isSameAs(deletePs);

		List<RegularStatement> regularStatements = regularStatementCaptor.getAllValues();

		assertThat(regularStatements).hasSize(4);
		assertThat(regularStatements.get(0).getQueryString()).isEqualTo("UPDATE counterTable SET count=count+? WHERE id=?;");
		assertThat(regularStatements.get(1).getQueryString()).isEqualTo("UPDATE counterTable SET count=count-? WHERE id=?;");
		assertThat(regularStatements.get(2).getQueryString()).isEqualTo("SELECT count FROM counterTable WHERE id=?;");
		assertThat(regularStatements.get(3).getQueryString()).isEqualTo("DELETE  FROM counterTable WHERE id=?;");
	}
}
