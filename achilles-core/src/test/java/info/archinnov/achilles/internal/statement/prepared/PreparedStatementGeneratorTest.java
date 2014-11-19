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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.decr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DECR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DELETE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.INCR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.SELECT;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.DELETE_ALL;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.SELECT_ALL;
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
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.type.Options.CASCondition;
import static info.archinnov.achilles.type.OptionsBuilder.ifConditions;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.core.querybuilder.Update.Conditions;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaStatementGenerator;
import org.junit.Before;
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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
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

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DirtyCheckChangeSet changeSet;

    @Captor
    ArgumentCaptor<String> queryCaptor;

    @Captor
    ArgumentCaptor<RegularStatement> regularStatementCaptor;

    @Captor
    ArgumentCaptor<Conditions> conditionsCaptor;

    @Before
    public void setUp() {
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.config().getTableName()).thenReturn("table");
        when(meta.config().getKeyspaceName()).thenReturn("ks");
    }

    @Test
    public void should_prepare_insert_ps() throws Exception {
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(nameMeta.getCQL3ColumnName()).thenReturn("name");
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareInsert(session, meta, asList(nameMeta), noOptions());

        assertThat(actual).isSameAs(ps);
        verify(idMeta.forStatementGeneration()).generateInsertPrimaryKey(isA(Insert.class), Mockito.eq(false));
        assertThat(queryCaptor.getValue()).isEqualTo("INSERT INTO ks.table(name) VALUES (:name) USING TTL :ttl;");
    }

    @Test
    public void should_prepare_select_field_ps() throws Exception {
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(nameMeta.structure().isCounter()).thenReturn(false);
        when(nameMeta.forStatementGeneration().prepareSelectField(isA(Selection.class))).thenReturn(select().column("name"));
        when(idMeta.forStatementGeneration().generateWhereClauseForSelect(Mockito.eq(fromNullable(nameMeta)), isA(Select.class)))
                .thenReturn(select("name").from("ks","table").where(eq("id",bindMarker("id"))));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectField(session, meta, nameMeta);

        assertThat(actual).isSameAs(ps);

        assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM ks.table WHERE id=:id;");
    }

    @Test
    public void should_exception_when_preparing_select_for_counter_type() throws Exception {

        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(nameMeta.structure().isCounter()).thenReturn(true);
        when(nameMeta.getPropertyName()).thenReturn("count");
        when(meta.getClassName()).thenReturn("entity");
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Cannot prepare statement for property 'count' of entity 'entity' because it is a counter type");

        generator.prepareSelectField(session, meta, nameMeta);
    }

    @Test
    public void should_prepare_update_fields_ps() throws Exception {
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta ageMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        final Assignments assignments = update("ks","table").with();

        when(nameMeta.structure().isStaticColumn()).thenReturn(false);
        when(ageMeta.structure().isStaticColumn()).thenReturn(false);
        when(nameMeta.forStatementGeneration().prepareUpdateField(conditionsCaptor.capture())).thenReturn(assignments);
        when(ageMeta.forStatementGeneration().prepareUpdateField(isA(Assignments.class))).thenReturn(assignments);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, false)).thenReturn(assignments.where(eq("id", bindMarker("id"))));
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareUpdateFields(session, meta, asList(nameMeta, ageMeta),
                ifConditions(new CASCondition("name", "John")).withTimestamp(100L));

        assertThat(actual).isSameAs(ps);

        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table IF name=:name;");
        assertThat(queryCaptor.getValue()).isEqualTo("UPDATE ks.table USING TTL :ttl AND TIMESTAMP :timestamp WHERE id=:id;");
    }

    @Test
    public void should_prepare_select_eager_ps_with_single_key() throws Exception {
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.forOperations().getColumnsMetaToLoad()).thenReturn(asList(nameMeta));
        when(nameMeta.forStatementGeneration().prepareSelectField(isA(Selection.class))).thenReturn(select().column("name"));
        when(meta.structure().hasOnlyStaticColumns()).thenReturn(false);
        when(idMeta.forStatementGeneration().generateWhereClauseForSelect(Mockito.eq(Optional.<PropertyMeta>absent()), isA(Select.class)))
                .thenReturn(select("name").from("ks","table").where(eq("id", bindMarker("id"))));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectAll(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM ks.table WHERE id=:id;");
    }

    @Test
    public void should_prepare_select_eager_ps_with_static_columns() throws Exception {
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.forOperations().getColumnsMetaToLoad()).thenReturn(asList(nameMeta));
        when(nameMeta.forStatementGeneration().prepareSelectField(isA(Selection.class))).thenReturn(select().column("name"));
        when(meta.structure().hasOnlyStaticColumns()).thenReturn(true);
        when(meta.getAllMetasExceptId()).thenReturn(asList(nameMeta));

        when(idMeta.forStatementGeneration().generateWhereClauseForSelect(Mockito.eq(Optional.fromNullable(nameMeta)), isA(Select.class)))
                .thenReturn(select("name").from("ks","table").where(eq("id", bindMarker("id"))));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        PreparedStatement actual = generator.prepareSelectAll(session, meta);

        assertThat(actual).isSameAs(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("SELECT name FROM ks.table WHERE id=:id;");
    }


    @Test
    public void should_delete_entity_having_single_key() throws Exception {
        when(meta.structure().hasOnlyStaticColumns()).thenReturn(true);

        when(idMeta.forStatementGeneration().generateWhereClauseForDelete(Mockito.eq(true), isA(Delete.class)))
                .thenReturn(delete().from("ks","table").where(eq("id", bindMarker("id"))));

        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        Map<String, PreparedStatement> actual = generator.prepareDeletePSs(session, meta);

        assertThat(actual).hasSize(1);
        assertThat(actual).containsValue(ps);
        assertThat(queryCaptor.getValue()).isEqualTo("DELETE FROM ks.table WHERE id=:id;");
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
                        update(ACHILLES_COUNTER_TABLE)
                        .with(incr(ACHILLES_COUNTER_VALUE, bindMarker()))
                        .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString());

        assertThat(queries.get(1)).isEqualTo(
                        update(ACHILLES_COUNTER_TABLE)
                        .with(decr(ACHILLES_COUNTER_VALUE, bindMarker()))
                        .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString());

        assertThat(queries.get(2)).isEqualTo(
                        select().column(ACHILLES_COUNTER_VALUE)
                        .from(ACHILLES_COUNTER_TABLE)
                        .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString());

        assertThat(queries.get(3)).isEqualTo(
                        delete()
                        .from(ACHILLES_COUNTER_TABLE)
                        .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                        .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString());

    }

    @Test
    public void should_prepare_clustered_counter_queries() throws Exception {
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMetaStatementGenerator statementGenerator = mock(PropertyMetaStatementGenerator.class, RETURNS_DEEP_STUBS);

        when(meta.getAllCounterMetas()).thenReturn(asList(counterMeta));
        when(counterMeta.forStatementGeneration()).thenReturn(statementGenerator);
        when(counterMeta.getPropertyName()).thenReturn("countProperty");
        when(counterMeta.getCQL3ColumnName()).thenReturn("count");
        when(counterMeta.structure().isStaticColumn()).thenReturn(false);


        when(statementGenerator.prepareCommonWhereClauseForUpdate(update("table").with(incr("count", bindMarker("count"))), false))
                .thenReturn(update("table").where());
        when(statementGenerator.prepareCommonWhereClauseForUpdate(update("table").with(decr("count", bindMarker("count"))), false))
                .thenReturn(update("table").where());
        when(statementGenerator.generateWhereClauseForSelect(Optional.fromNullable(counterMeta), select("count").from("table")))
                .thenReturn(select("count").from("table").where(eq("id", bindMarker("id"))));
        when(statementGenerator.generateWhereClauseForSelect(Optional.<PropertyMeta>absent(), select().from("table")))
                .thenReturn(select().from("table"));
        when(statementGenerator.generateWhereClauseForDelete(false, delete().from("table")))
                .thenReturn(delete().from("table"));


        PreparedStatement incrPs = mock(PreparedStatement.class);
        PreparedStatement decrPs = mock(PreparedStatement.class);
        PreparedStatement selectPs = mock(PreparedStatement.class);
        PreparedStatement selectAllPs = mock(PreparedStatement.class);
        PreparedStatement deletePs = mock(PreparedStatement.class);

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(incrPs, decrPs, selectPs, selectAllPs, deletePs);

        Map<CQLQueryType, Map<String, PreparedStatement>> actual = generator.prepareClusteredCounterQueryMap(session,meta);

        assertThat(actual.get(INCR).get("countProperty")).isSameAs(incrPs);
        assertThat(actual.get(DECR).get("countProperty")).isSameAs(decrPs);
        assertThat(actual.get(SELECT).get("countProperty")).isSameAs(selectPs);
        assertThat(actual.get(SELECT).get(SELECT_ALL.name())).isSameAs(selectAllPs);
        assertThat(actual.get(DELETE).get(DELETE_ALL.name())).isSameAs(deletePs);

        List<RegularStatement> regularStatements = regularStatementCaptor.getAllValues();

        assertThat(regularStatements).hasSize(5);

    }

    @Test
    public void should_prepare_statement_to_remove_all_collection_and_map_with_options() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(REMOVE_COLLECTION_OR_MAP);
        when(changeSet.generateUpdateForRemoveAll(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet,
                ifConditions(new CASCondition("name", "John")).withTimestamp(100L));

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl AND TIMESTAMP :timestamp;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table IF name=:name;");

    }

    @Test
    public void should_prepare_statement_to_add_elements_to_set() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(ADD_TO_SET);
        when(changeSet.generateUpdateForAddedElements(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test
    public void should_prepare_statement_to_remove_elements_from_set() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_SET);
        when(changeSet.generateUpdateForRemovedElements(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test
    public void should_prepare_statement_to_append_elements_to_list() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(APPEND_TO_LIST);
        when(changeSet.generateUpdateForAppendedElements(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test
    public void should_prepare_statement_to_prepend_elements_to_list() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(PREPEND_TO_LIST);
        when(changeSet.generateUpdateForPrependedElements(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test
    public void should_prepare_statement_to_remove_elements_from_list() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST);
        when(changeSet.generateUpdateForRemoveListElements(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_prepare_statement_to_set_element_at_index_from_list() throws Exception {
        //Given
        when(changeSet.getChangeType()).thenReturn(SET_TO_LIST_AT_INDEX);

        //When
        generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_prepare_statement_to_remove_element_at_index_from_list() throws Exception {
        //Given
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST_AT_INDEX);

        //When
        generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());
    }

    @Test
    public void should_prepare_statement_to_add_entries_to_map() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(ADD_TO_MAP);
        when(changeSet.generateUpdateForAddedEntries(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test
    public void should_prepare_statement_to_remove_entry_from_map() throws Exception {
        //Given
        final Assignments assignments = update("table").with();

        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_MAP);
        when(changeSet.generateUpdateForRemovedKey(conditionsCaptor.capture())).thenReturn(assignments);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(true);
        when(idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, true)).thenReturn(update("ks","table").where());

        when(session.prepare(regularStatementCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareCollectionAndMapUpdate(session, meta, changeSet, noOptions());

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(regularStatementCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL :ttl;");
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table;");
    }

    @Test
    public void should_prepare_select_slice_query() throws Exception {
        //Given
        final ArgumentCaptor<Select> selectCaptor = ArgumentCaptor.forClass(Select.class);
        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final Select select = select().from("test").where(eq("id", 10)).limit(1);
        final PropertyMeta pm = completeBean(Void.class, Long.class)
                .propertyName("name").cqlColumnName("name").type(SIMPLE).build();

        when(sliceQueryProperties.getEntityMeta()).thenReturn(meta);
        when(meta.forOperations().getColumnsMetaToLoad()).thenReturn(asList(pm));
        when(meta.config().getQualifiedTableName()).thenReturn("table");
        when(sliceQueryProperties.generateWhereClauseForSelect(selectCaptor.capture())).thenReturn(select);
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareSelectSliceQuery(session, sliceQueryProperties);

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(selectCaptor.getValue().getQueryString()).isEqualTo("SELECT name FROM ks.table;");
        assertThat(queryCaptor.getValue()).isEqualTo("SELECT * FROM test WHERE id=10 LIMIT 1;");
    }

    @Test
    public void should_prepare_for_delete_slice_query() throws Exception {
        //Given
        final ArgumentCaptor<Delete> deleteCaptor = ArgumentCaptor.forClass(Delete.class);
        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        final Delete.Where delete = delete().from("ks","table").where();
        when(sliceQueryProperties.getEntityMeta().config().getTableName()).thenReturn("table");
        when(sliceQueryProperties.getEntityMeta().config().getKeyspaceName()).thenReturn("ks");
        when(sliceQueryProperties.generateWhereClauseForDelete(deleteCaptor.capture())).thenReturn(delete);
        when(session.prepare(queryCaptor.capture())).thenReturn(ps);

        //When
        final PreparedStatement actual = generator.prepareDeleteSliceQuery(session, sliceQueryProperties);

        //Then
        assertThat(actual).isSameAs(ps);
        assertThat(deleteCaptor.getValue().getQueryString()).isEqualTo("DELETE FROM ks.table;");
        assertThat(queryCaptor.getValue()).isEqualTo("DELETE FROM ks.table;");
    }
}
