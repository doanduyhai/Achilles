package info.archinnov.achilles.statement;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
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
import testBuilders.PropertyMetaTestBuilder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

/**
 * CQLPreparedStatementHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPreparedStatementGeneratorTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private CQLPreparedStatementGenerator generator = new CQLPreparedStatementGenerator();

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

        PropertyMeta<?, ?> proxyTypeMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("proxyType")
                .type(PropertyType.WIDE_MAP)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, "name", nameMeta, "proxyType", proxyTypeMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareInsertPS(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("INSERT INTO table(id,name) VALUES (?,?);");
    }

    @Test
    public void should_prepare_insert_ps_with_clustered_id() throws Exception
    {
        List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .compNames("id", "a", "b")
                .type(PropertyType.COMPOUND_ID)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .build();

        allMetas.add(nameMeta);
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareInsertPS(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo(
                "INSERT INTO table(id,a,b,name) VALUES (?,?,?,?);");
    }

    @Test
    public void should_prepare_select_field_ps() throws Exception
    {

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

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectFieldPS(session, meta, nameMeta);

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM table WHERE id=?;");
    }

    @Test
    public void should_prepare_select_field_ps_for_clustered_id() throws Exception
    {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.COMPOUND_ID)
                .compNames("id", "a", "b")
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectFieldPS(session, meta, idMeta);

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo(
                "SELECT id,a,b FROM table WHERE id=? AND a=? AND b=?;");
    }

    @Test
    public void should_prepare_update_fields_ps() throws Exception
    {

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

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareUpdateFields(session, meta,
                Arrays.<PropertyMeta<?, ?>> asList(nameMeta, ageMeta));

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo("UPDATE table SET name=?,age=? WHERE id=?;");
    }

    @Test
    public void should_prepare_update_fields_with_clustered_id_ps() throws Exception
    {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .compNames("id", "a", "b")
                .type(PropertyType.COMPOUND_ID)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareUpdateFields(session, meta,
                Arrays.<PropertyMeta<?, ?>> asList(nameMeta, ageMeta));

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo(
                "UPDATE table SET name=?,age=? WHERE id=? AND a=? AND b=?;");
    }

    @Test
    public void should_exception_when_preparing_select_for_proxy_type() throws Exception
    {

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("widemap")
                .type(PropertyType.WIDE_MAP)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("entity");

        exception.expect(IllegalArgumentException.class);
        exception
                .expectMessage("Cannot prepare statement for property 'widemap' of entity 'entity' because it is of proxy type");

        generator.prepareSelectFieldPS(session, meta, nameMeta);

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

        PreparedStatement actual = generator.prepareSelectEagerPS(session, meta);

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
                .type(PropertyType.COMPOUND_ID)
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

        PreparedStatement actual = generator.prepareSelectEagerPS(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo(
                "SELECT name FROM table WHERE id=? AND a=? AND b=?;");
    }

    @Test
    public void should_remove_entity_having_single_key() throws Exception
    {

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
    public void should_remove_entity_having_clustered_key() throws Exception
    {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .compNames("id", "a", "b")
                .type(PropertyType.COMPOUND_ID)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", nameMeta));
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

        assertThat(actual).hasSize(1);
        assertThat(actual).containsValue(ps);
        assertThat(queryCaptor.getValue()).isEqualTo(
                "DELETE  FROM table WHERE id=? AND a=? AND b=?;");
    }

    @Test
    public void should_remove_entity_having_wide_map() throws Exception
    {

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

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

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

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

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

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

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

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

        assertThat(actual).hasSize(2);
        assertThat(actual).containsKey("table");
        assertThat(actual).containsKey(AchillesCounter.CQL_COUNTER_TABLE);
        assertThat(actual).containsValue(ps);
        assertThat(actual).containsValue(ps2);
        assertThat(queryCaptor.getAllValues()).contains(
                "DELETE  FROM table WHERE id=?;",
                "DELETE  FROM " + AchillesCounter.CQL_COUNTER_TABLE + " WHERE "
                        + AchillesCounter.CQL_COUNTER_FQCN + "=? AND "
                        + AchillesCounter.CQL_COUNTER_PRIMARY_KEY + "=?;");
    }

    @Test
    public void should_prepare_simple_counter_queries() throws Exception
    {
        PreparedStatement incrPs = mock(PreparedStatement.class);
        PreparedStatement decrPs = mock(PreparedStatement.class);
        PreparedStatement selectPs = mock(PreparedStatement.class);
        PreparedStatement deletePs = mock(PreparedStatement.class);

        when(session.prepare(queryCaptor.capture())).thenReturn(incrPs, decrPs, selectPs, deletePs);

        Map<CQLQueryType, PreparedStatement> actual = generator
                .prepareSimpleCounterQueryMap(session);

        assertThat(actual.get(INCR)).isSameAs(incrPs);
        assertThat(actual.get(DECR)).isSameAs(decrPs);
        assertThat(actual.get(SELECT)).isSameAs(selectPs);
        assertThat(actual.get(DELETE)).isSameAs(deletePs);

        List<String> queries = queryCaptor.getAllValues();

        assertThat(queries).hasSize(4);
        assertThat(queries.get(0)).isEqualTo(
                "UPDATE " + CQL_COUNTER_TABLE + " SET " + CQL_COUNTER_VALUE + " = "
                        + CQL_COUNTER_VALUE + " + ? WHERE " + CQL_COUNTER_FQCN + " = ? AND "
                        + CQL_COUNTER_PRIMARY_KEY + " = ? AND " + CQL_COUNTER_PROPERTY_NAME
                        + " = ?");
        assertThat(queries.get(1)).isEqualTo(
                "UPDATE " + CQL_COUNTER_TABLE + " SET " + CQL_COUNTER_VALUE + " = "
                        + CQL_COUNTER_VALUE + " - ? WHERE " + CQL_COUNTER_FQCN + " = ? AND "
                        + CQL_COUNTER_PRIMARY_KEY + " = ? AND " + CQL_COUNTER_PROPERTY_NAME
                        + " = ?");
        assertThat(queries.get(2)).isEqualTo(
                "SELECT " + CQL_COUNTER_VALUE + " FROM " + CQL_COUNTER_TABLE + " WHERE "
                        + CQL_COUNTER_FQCN + " = ? AND " + CQL_COUNTER_PRIMARY_KEY + " = ? AND "
                        + CQL_COUNTER_PROPERTY_NAME + " = ?");
        assertThat(queries.get(3)).isEqualTo(
                "DELETE FROM " + CQL_COUNTER_TABLE + " WHERE " + CQL_COUNTER_FQCN + " = ? AND "
                        + CQL_COUNTER_PRIMARY_KEY + " = ? AND " + CQL_COUNTER_PROPERTY_NAME
                        + " = ?");

    }
}
