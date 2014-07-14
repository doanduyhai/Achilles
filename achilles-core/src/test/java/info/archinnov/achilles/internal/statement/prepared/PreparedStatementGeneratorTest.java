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
package info.archinnov.achilles.internal.statement.prepared;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DECR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DELETE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.INCR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.SELECT;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.DELETE_ALL;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.APPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.PREPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_COLLECTION_OR_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;
import static info.archinnov.achilles.test.builders.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.type.Options.CASCondition;
import static info.archinnov.achilles.type.OptionsBuilder.ifConditions;
import static info.archinnov.achilles.type.OptionsBuilder.ifNotExists;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.query.slice.SliceQueryProperties;

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

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private SliceQueryProperties sliceQueryProperties;

    @Mock
    private EntityMeta meta;

    @Captor
    ArgumentCaptor<String> queryCaptor;

    @Captor
    ArgumentCaptor<RegularStatement> regularStatementCaptor;

    @Test
    public void should_prepare_insert_ps() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.SIMPLE).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareInsert(session, meta, asList(nameMeta), noOptions());

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("INSERT INTO table(id,name) VALUES (:id,:name) USING TTL :ttl;");
    }

    @Test
    public void should_prepare_insert_ps_with_clustered_id_and_options() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").compNames("id", "a", "b")
                .type(PropertyType.EMBEDDED_ID).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareInsert(session, meta, asList(nameMeta), ifNotExists().withTimestamp(100L));

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo(
                "INSERT INTO table(id,a,b,name) VALUES (:id,:a,:b,:name) IF NOT EXISTS USING TTL :ttl AND TIMESTAMP :timestamp;");
    }

    @Test
    public void should_prepare_select_field_ps() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.SIMPLE).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectField(session, meta, nameMeta);

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM table WHERE id=:id;");
    }

    @Test
    public void should_prepare_select_field_ps_for_clustered_id() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.EMBEDDED_ID)
                .compNames("id", "a", "b").build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectField(session, meta, idMeta);

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo("SELECT id,a,b FROM table WHERE id=:id AND a=:a AND b=:b;");
    }

    @Test
    public void should_prepare_update_fields_ps() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.SIMPLE).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        PropertyMeta ageMeta = completeBean(Void.class, String.class).field("age").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareUpdateFields(session, meta, asList(nameMeta, ageMeta),
                ifConditions(new CASCondition("name", "John")).withTimestamp(100L));

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo(
                "UPDATE table USING TTL :ttl AND TIMESTAMP :timestamp SET name=:name,age=:age WHERE id=:id IF name=:name;");
    }

    @Test
    public void should_prepare_update_fields_with_clustered_id_ps() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").compNames("id", "a", "b")
                .type(PropertyType.EMBEDDED_ID).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        PropertyMeta ageMeta = completeBean(Void.class, String.class).field("age").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareUpdateFields(session, meta, asList(nameMeta, ageMeta), noOptions());

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo(
                "UPDATE table USING TTL :ttl SET name=:name,age=:age WHERE id=:id AND a=:a AND b=:b;");
    }

    @Test
    public void should_exception_when_preparing_select_for_counter_type() throws Exception {

        PropertyMeta nameMeta = completeBean(Void.class, Long.class).field("count").type(PropertyType.COUNTER).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("entity");

        exception.expect(IllegalArgumentException.class);
        exception
                .expectMessage("Cannot prepare statement for property 'count' of entity 'entity' because it is a counter type");

        generator.prepareSelectField(session, meta, nameMeta);

    }

    @Test
    public void should_prepare_select_eager_ps_with_single_key() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.SIMPLE).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setAllMetasExceptIdAndCounters(asList(nameMeta));
        meta.setAllMetasExceptCounters(asList(nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectAll(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM table WHERE id=:id;");
    }

    @Test
    public void should_prepare_select_eager_ps_with_clustered_key() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").compNames("id", "a", "b")
                .type(PropertyType.EMBEDDED_ID).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setAllMetasExceptCounters(asList(idMeta, nameMeta));
        meta.setClusteredCounter(false);

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectAll(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("SELECT id,a,b,name FROM table WHERE id=:id AND a=:a AND b=:b;");
    }

    @Test
    public void should_remove_entity_having_single_key() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.SIMPLE).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

        assertThat(actual).hasSize(1);
        assertThat(actual).containsValue(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("DELETE FROM table WHERE id=:id;");
    }

    @Test
    public void should_remove_entity_having_clustered_key() throws Exception {

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").compNames("id", "a", "b")
                .type(PropertyType.EMBEDDED_ID).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(PropertyType.SIMPLE).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.of("name", nameMeta));
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

        assertThat(actual).hasSize(1);
        assertThat(actual).containsValue(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("DELETE FROM table WHERE id=:id AND a=:a AND b=:b;");
    }

    @Test
    public void should_remove_entity_having_counter() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(PropertyType.SIMPLE).build();

        PropertyMeta nameMeta = completeBean(UUID.class, String.class).field("count").type(PropertyType.COUNTER)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.of("name", nameMeta));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps, ps2);

        Map<String, PreparedStatement> actual = generator.prepareRemovePSs(session, meta);

        assertThat(actual).hasSize(1);
        assertThat(actual).containsKey("table");
        assertThat(actual).containsValue(ps);
        assertThat(queryCaptor.getAllValues()).containsOnly("DELETE FROM table WHERE id=:id;");
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
                "UPDATE " + CQL_COUNTER_TABLE + " SET " + CQL_COUNTER_VALUE + "=" + CQL_COUNTER_VALUE + "+? WHERE "
                        + CQL_COUNTER_FQCN + "=? AND " + CQL_COUNTER_PRIMARY_KEY + "=? AND "
                        + CQL_COUNTER_PROPERTY_NAME + "=?;");
        assertThat(queries.get(1)).isEqualTo(
                "UPDATE " + CQL_COUNTER_TABLE + " SET " + CQL_COUNTER_VALUE + "=" + CQL_COUNTER_VALUE + "-? WHERE "
                        + CQL_COUNTER_FQCN + "=? AND " + CQL_COUNTER_PRIMARY_KEY + "=? AND "
                        + CQL_COUNTER_PROPERTY_NAME + "=?;");
        assertThat(queries.get(2)).isEqualTo(
                "SELECT " + CQL_COUNTER_VALUE + " FROM " + CQL_COUNTER_TABLE + " WHERE " + CQL_COUNTER_FQCN
                        + "=? AND " + CQL_COUNTER_PRIMARY_KEY + "=? AND " + CQL_COUNTER_PROPERTY_NAME + "=?;");
        assertThat(queries.get(3)).isEqualTo(
                "DELETE FROM " + CQL_COUNTER_TABLE + " WHERE " + CQL_COUNTER_FQCN + "=? AND "
                        + CQL_COUNTER_PRIMARY_KEY + "=? AND " + CQL_COUNTER_PROPERTY_NAME + "=?;");

    }

    @Test
    public void should_prepare_clustered_counter_queries() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();

        PropertyMeta counterMeta = completeBean(Void.class, String.class).field("count").type(COUNTER).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("counterTable");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "counter", counterMeta));

        PreparedStatement incrPs = mock(PreparedStatement.class);
        PreparedStatement decrPs = mock(PreparedStatement.class);
        PreparedStatement selectPs = mock(PreparedStatement.class);
        PreparedStatement deletePs = mock(PreparedStatement.class);

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(incrPs, decrPs, selectPs, deletePs);

        Map<CQLQueryType, Map<String, PreparedStatement>> actual = generator.prepareClusteredCounterQueryMap(session,
                meta);

        assertThat(actual.get(INCR).get("count")).isSameAs(incrPs);
        assertThat(actual.get(DECR).get("count")).isSameAs(decrPs);
        assertThat(actual.get(SELECT).get("count")).isSameAs(selectPs);
        assertThat(actual.get(DELETE).get(DELETE_ALL.name())).isSameAs(deletePs);

        List<RegularStatement> regularStatements = regularStatementCaptor.getAllValues();

        assertThat(regularStatements).hasSize(5);
        assertThat(regularStatements.get(0).getQueryString()).isEqualTo(
                "UPDATE counterTable SET count=count+:count WHERE id=:id;");
        assertThat(regularStatements.get(1).getQueryString()).isEqualTo(
                "UPDATE counterTable SET count=count-:count WHERE id=:id;");
        assertThat(regularStatements.get(2).getQueryString()).isEqualTo("SELECT count FROM counterTable WHERE id=:id;");
        assertThat(regularStatements.get(3).getQueryString()).isEqualTo("SELECT * FROM counterTable WHERE id=:id;");
        assertThat(regularStatements.get(4).getQueryString()).isEqualTo("DELETE FROM counterTable WHERE id=:id;");
    }

    @Test
    public void should_prepare_statement_to_remove_all_collection_and_map_with_options() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta setMeta = completeBean(Void.class, String.class).field("followers").type(SET).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "followers", setMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(setMeta, REMOVE_COLLECTION_OR_MAP);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet,
                ifConditions(new CASCondition("name", "John")).withTimestamp(100L));

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl AND TIMESTAMP :timestamp SET followers=:followers WHERE id=:id IF name=:name;");

    }

    @Test
    public void should_prepare_statement_to_add_elements_to_set() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta setMeta = completeBean(Void.class, String.class).field("followers").type(SET).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "followers", setMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(setMeta, ADD_TO_SET);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl SET followers=followers+:followers WHERE id=:id;");

    }

    @Test
    public void should_prepare_statement_to_remove_elements_from_set() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta setMeta = completeBean(Void.class, String.class).field("followers").type(SET).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "followers", setMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(setMeta, REMOVE_FROM_SET);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl SET followers=followers-:followers WHERE id=:id;");

    }

    @Test
    public void should_prepare_statement_to_append_elements_to_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta listMeta = completeBean(Void.class, String.class).field("friends").type(LIST).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "friends", listMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(listMeta, APPEND_TO_LIST);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl SET friends=friends+:friends WHERE id=:id;");
    }

    @Test
    public void should_prepare_statement_to_prepend_elements_to_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta listMeta = completeBean(Void.class, String.class).field("friends").type(LIST).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "friends", listMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(listMeta, PREPEND_TO_LIST);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl SET friends=:friends+friends WHERE id=:id;");
    }

    @Test
    public void should_prepare_statement_to_remove_elements_from_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta listMeta = completeBean(Void.class, String.class).field("friends").type(LIST).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "friends", listMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(listMeta, REMOVE_FROM_LIST);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE table USING TTL :ttl SET friends=friends-:friends WHERE id=:id;");
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_prepare_statement_to_set_element_at_index_from_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta listMeta = completeBean(Void.class, String.class).field("friends").type(LIST).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "friends", listMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(listMeta, SET_TO_LIST_AT_INDEX);

        //When
        generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_prepare_statement_to_remove_element_at_index_from_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta listMeta = completeBean(Void.class, String.class).field("friends").type(LIST).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "friends", listMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(listMeta, REMOVE_FROM_LIST_AT_INDEX);

        //When
        generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());
    }

    @Test
    public void should_prepare_statement_to_add_entries_to_map() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta mapMeta = completeBean(Integer.class, String.class).field("preferences").type(MAP).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "preferences", mapMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(mapMeta, ADD_TO_MAP);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl SET preferences=preferences+:preferences WHERE id=:id;");
    }

    @Test
    public void should_prepare_statement_to_remove_entry_from_map() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").type(ID).build();
        PropertyMeta mapMeta = completeBean(Integer.class, String.class).field("preferences").type(MAP).build();
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "preferences", mapMeta));
        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);
        DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(mapMeta, REMOVE_FROM_MAP);
        Whitebox.setInternalState(changeSet, "mapChanges", ImmutableMap.of(1, "a"));

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString())
                .isEqualTo("UPDATE table USING TTL :ttl SET preferences[:key]=:nullValue WHERE id=:id;");
    }

    @Test
    public void should_prepare_select_slice_query() throws Exception {
        //Given
        final ArgumentCaptor<Select> selectCaptor = ArgumentCaptor.forClass(Select.class);
        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final Select select = select().from("test").where(eq("id", 10)).limit(1);
        final PropertyMeta pm = completeBean(Void.class, Long.class).field("name").type(SIMPLE).build();

        when(sliceQueryProperties.getEntityMeta()).thenReturn(meta);
        when(meta.getColumnsMetaToLoad()).thenReturn(asList(pm));
        when(meta.getTableName()).thenReturn("table");
        when(sliceQueryProperties.generateWhereClauseForSelect(selectCaptor.capture())).thenReturn(select);
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareSelectSliceQuery(session, sliceQueryProperties);

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(selectCaptor.getValue().getQueryString()).isEqualTo("SELECT name FROM table;");
        assertThat(queryCaptor.getValue()).isEqualTo("SELECT * FROM test WHERE id=10 LIMIT 1;");
    }

    @Test
    public void should_prepare_for_delete_slice_query() throws Exception {
        //Given
        final ArgumentCaptor<Delete> deleteCaptor = ArgumentCaptor.forClass(Delete.class);
        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final Delete.Where delete = delete().from("table").where();
        when(sliceQueryProperties.getEntityMeta().getTableName()).thenReturn("table");
        when(sliceQueryProperties.generateWhereClauseForDelete(deleteCaptor.capture())).thenReturn(delete);
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareDeleteSliceQuery(session, sliceQueryProperties);

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(deleteCaptor.getValue().getQueryString()).isEqualTo("DELETE FROM table;");
        assertThat(queryCaptor.getValue()).isEqualTo("DELETE FROM table;");
    }
}
