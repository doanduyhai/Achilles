package info.archinnov.achilles.helper;

import static info.archinnov.achilles.context.CQLDaoContext.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * CQLPreparedStatementHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPreparedStatementHelperTest
{
	private CQLPreparedStatementHelper helper = new CQLPreparedStatementHelper();

	@Mock
	private Session session;

	@Mock
	private PreparedStatement ps;

	@Mock
	private PreparedStatement ps2;

	@Captor
	ArgumentCaptor<String> queryCaptor;

	@Test
	public void should_prepare_insert_ps() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> proxyTypeMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("proxyType")
				.type(PropertyType.WIDE_MAP)
				.build();

		allMetas.add(nameMeta);
		allMetas.add(proxyTypeMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = helper.prepareInsertPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("INSERT INTO table(name) VALUES (?);");
	}

	@Test
	public void should_prepare_select_for_existence_check() throws Exception
	{
		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = helper.prepareSelectForExistenceCheckPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("SELECT id FROM table WHERE id=?;");
	}

	@Test
	public void should_prepare_select_eager_ps_with_single_key() throws Exception
	{
		List<PropertyMeta<?, ?>> eagerMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		eagerMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setEagerMetas(eagerMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = helper.prepareSelectEagerPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM table WHERE id=?;");
	}

	@Test
	public void should_prepare_select_eager_ps_with_clustered_key() throws Exception
	{
		List<PropertyMeta<?, ?>> eagerMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.compNames("id", "a", "b")
				.type(PropertyType.CLUSTERED_KEY)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		eagerMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setEagerMetas(eagerMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = helper.prepareSelectEagerPS(session, meta);

		assertThat(actual).isSameAs(ps);
		assertThat(queryCaptor.getValue()).isEqualTo(
				"SELECT name FROM table WHERE id=? AND a=? AND b=?;");
	}

	@Test
	public void should_remove_entity_having_single_key() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		Map<String, PreparedStatement> actual = helper.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(1);
		assertThat(actual).containsValue(ps);
		assertThat(queryCaptor.getValue()).isEqualTo("DELETE  FROM table WHERE id=?;");
	}

	@Test
	public void should_remove_entity_having_clustered_key() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.compNames("id", "a", "b")
				.type(PropertyType.CLUSTERED_KEY)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps);

		Map<String, PreparedStatement> actual = helper.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(1);
		assertThat(actual).containsValue(ps);
		assertThat(queryCaptor.getValue()).isEqualTo(
				"DELETE  FROM table WHERE id=? AND a=? AND b=?;");
	}

	@Test
	public void should_remove_entity_having_wide_map() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(UUID.class, String.class)
				.field("widemap")
				.type(PropertyType.WIDE_MAP)
				.externalTable("external_table")
				.build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

		Map<String, PreparedStatement> actual = helper.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(2);
		assertThat(actual).containsKey("table");
		assertThat(actual).containsKey("external_table");
		assertThat(actual).containsValue(ps);
		assertThat(actual).containsValue(ps2);
		assertThat(queryCaptor.getAllValues()).contains("DELETE  FROM table WHERE id=?;",
				"DELETE  FROM external_table WHERE id=?;");
	}

	@Test
	public void should_remove_entity_having_join_wide_map() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(UUID.class, String.class)
				.field("join_widemap")
				.type(PropertyType.JOIN_WIDE_MAP)
				.externalTable("join_external_table")
				.build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

		Map<String, PreparedStatement> actual = helper.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(2);
		assertThat(actual).containsKey("table");
		assertThat(actual).containsKey("join_external_table");
		assertThat(actual).containsValue(ps);
		assertThat(actual).containsValue(ps2);
		assertThat(queryCaptor.getAllValues()).contains("DELETE  FROM table WHERE id=?;",
				"DELETE  FROM join_external_table WHERE id=?;");
	}

	@Test
	public void should_remove_entity_having_counter_wide_map() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(UUID.class, String.class)
				.field("counter_widemap")
				.type(PropertyType.COUNTER_WIDE_MAP)
				.externalTable("counter_external_table")
				.build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

		Map<String, PreparedStatement> actual = helper.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(2);
		assertThat(actual).containsKey("table");
		assertThat(actual).containsKey("counter_external_table");
		assertThat(actual).containsValue(ps);
		assertThat(actual).containsValue(ps2);
		assertThat(queryCaptor.getAllValues()).contains("DELETE  FROM table WHERE id=?;",
				"DELETE  FROM counter_external_table WHERE id=?;");
	}

	@Test
	public void should_remove_entity_having_counter() throws Exception
	{
		List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(UUID.class, String.class)
				.field("counter")
				.type(PropertyType.COUNTER)
				.build();

		allMetas.add(nameMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setIdMeta(idMeta);
		meta.setAllMetas(allMetas);

		when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

		Map<String, PreparedStatement> actual = helper.prepareRemovePSs(session, meta);

		assertThat(actual).hasSize(2);
		assertThat(actual).containsKey("table");
		assertThat(actual).containsKey(ACHILLES_COUNTER_TABLE);
		assertThat(actual).containsValue(ps);
		assertThat(actual).containsValue(ps2);
		assertThat(queryCaptor.getAllValues()).contains(
				"DELETE  FROM table WHERE id=?;",
				"DELETE  FROM " + ACHILLES_COUNTER_TABLE + " WHERE " + ACHILLES_COUNTER_FQCN
						+ "=? AND " + ACHILLES_COUNTER_PK + "=?;");
	}
}
