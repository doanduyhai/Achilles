package info.archinnov.achilles.internal.metadata.holder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.datastax.driver.core.querybuilder.Update.Conditions;
import com.datastax.driver.core.querybuilder.Update.Where;
import com.google.common.base.Optional;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaStatementGeneratorTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EmbeddedIdProperties embeddedIdProperties;

    private PropertyMetaStatementGenerator view;

    @Before
    public void setUp() {
        view = new PropertyMetaStatementGenerator(meta);
        when(meta.getEmbeddedIdProperties()).thenReturn(embeddedIdProperties);
        when(meta.getEntityClassName()).thenReturn("entity");
    }

    @Test
    public void should_prepare_insert_primary_key_for_embedded_id() throws Exception {
        //Given
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getCQL3ComponentNames()).thenReturn(asList("id", "name"));
        Insert insert = QueryBuilder.insertInto("table");

        //When
        final Insert actual = view.generateInsertPrimaryKey(insert, false);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("INSERT INTO table(id,name) VALUES (:id,:name);");
    }

    @Test
    public void should_prepare_insert_primary_key_for_embedded_id_with_only_static_columns() throws Exception {
        //Given
        PropertyMeta meta1 = mock(PropertyMeta.class);
        when(meta1.getCQL3ColumnName()).thenReturn("id");

        PartitionComponents partitionComponents = new PartitionComponents(asList(meta1));
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(meta.getEmbeddedIdProperties().getPartitionComponents()).thenReturn(partitionComponents);
        Insert insert = QueryBuilder.insertInto("table");

        //When
        final Insert actual = view.generateInsertPrimaryKey(insert, true);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("INSERT INTO table(id) VALUES (:id);");
    }

    @Test
    public void should_prepare_insert_primary_key_for_simple_id() throws Exception {
        //Given
        when(meta.structure().isEmbeddedId()).thenReturn(false);
        when(meta.getCQL3ColumnName()).thenReturn("id");
        Insert insert = QueryBuilder.insertInto("table");

        //When
        final Insert actual = view.generateInsertPrimaryKey(insert, false);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("INSERT INTO table(id) VALUES (:id);");
    }

    @Test
    public void should_prepare_where_clause_for_select_with_embedded_id() throws Exception {
        //Given
        Optional<PropertyMeta> pmO = Optional.absent();
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getCQL3ComponentNames()).thenReturn(asList("id", "name"));
        Select select = QueryBuilder.select().from("table");

        //When
        final RegularStatement actual = view.generateWhereClauseForSelect(pmO, select);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id AND name=:name;");
    }

    @Test
    public void should_prepare_where_clause_for_select_with_embedded_id_and_static_column() throws Exception {
        //Given
        PropertyMeta staticMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(staticMeta.structure().isStaticColumn()).thenReturn(true);

        Optional<PropertyMeta> pmO = Optional.fromNullable(staticMeta);

        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getPartitionComponents().getCQL3ComponentNames()).thenReturn(asList("id"));
        Select select = QueryBuilder.select().from("table");

        //When
        final RegularStatement actual = view.generateWhereClauseForSelect(pmO, select);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id;");
    }

    @Test
    public void should_prepare_where_clause_for_select_with_simple_id() throws Exception {
        //Given
        Optional<PropertyMeta> pmO = Optional.absent();
        when(meta.structure().isEmbeddedId()).thenReturn(false);
        when(meta.getCQL3ColumnName()).thenReturn("id");
        Select select = QueryBuilder.select().from("table");

        //When
        final RegularStatement actual = view.generateWhereClauseForSelect(pmO, select);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("SELECT * FROM table WHERE id=:id;");
    }


    @Test
    public void should_prepare_where_clause_for_delete_with_embedded_id() throws Exception {
        //Given
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getCQL3ComponentNames()).thenReturn(asList("id", "name"));
        Delete delete = QueryBuilder.delete().from("table");

        //When
        final RegularStatement actual = view.generateWhereClauseForDelete(false, delete);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("DELETE FROM table WHERE id=:id AND name=:name;");
    }

    @Test
    public void should_prepare_where_clause_for_delete_with_embedded_id_and_static_column() throws Exception {
        //Given
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getPartitionComponents().getCQL3ComponentNames()).thenReturn(asList("id"));
        Delete delete = QueryBuilder.delete().from("table");

        //When
        final RegularStatement actual = view.generateWhereClauseForDelete(true, delete);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("DELETE FROM table WHERE id=:id;");
    }

    @Test
    public void should_prepare_where_clause_for_delete_with_simple_id() throws Exception {
        //Given
        when(meta.structure().isEmbeddedId()).thenReturn(false);
        when(meta.getCQL3ColumnName()).thenReturn("id");
        Delete delete = QueryBuilder.delete().from("table");

        //When
        final RegularStatement actual = view.generateWhereClauseForDelete(true, delete);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("DELETE FROM table WHERE id=:id;");
    }

    @Test
    public void should_prepare_update_fields_with_conditions() throws Exception {
        //Given
        when(meta.getCQL3ColumnName()).thenReturn("name");
        final Conditions conditions = update("table").onlyIf();

        //When
        final Assignments actual = view.prepareUpdateField(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET name=:name;");
    }

    @Test
    public void should_prepare_update_fields_with_assignments() throws Exception {
        //Given
        when(meta.getCQL3ColumnName()).thenReturn("name");
        final Assignments assignments = update("table").with();

        //When
        final Assignments actual = view.prepareUpdateField(assignments);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET name=:name;");
    }

    @Test
    public void should_prepare_common_where_clause_for_update_with_embedded_id() throws Exception {
        //Given
        final Assignments assignments = update("table").with();
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getCQL3ComponentNames()).thenReturn(asList("id", "name"));

        //When
        final Where actual = view.prepareCommonWhereClauseForUpdate(assignments, false);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table WHERE id=:id AND name=:name;");
    }

    @Test
    public void should_prepare_common_where_clause_for_update_with_embedded_id_and_static_column() throws Exception {
        //Given
        final Assignments assignments = update("table").with();
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getPartitionComponents().getCQL3ComponentNames()).thenReturn(asList("id"));

        //When
        final Where actual = view.prepareCommonWhereClauseForUpdate(assignments, true);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table WHERE id=:id;");
    }

    @Test
    public void should_prepare_common_where_clause_for_update_with_simple_id() throws Exception {
        //Given
        final Assignments assignments = update("table").with();
        when(meta.structure().isEmbeddedId()).thenReturn(false);
        when(meta.getCQL3ColumnName()).thenReturn("id");

        //When
        final Where actual = view.prepareCommonWhereClauseForUpdate(assignments, true);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table WHERE id=:id;");
    }

    @Test
    public void should_generate_where_clause_for_update_with_embedded_id() throws Exception {
        //Given
        Object entity = new Object();
        CompleteBean pk = new CompleteBean();
        PropertyMeta pm = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.forValues().getPrimaryKey(entity)).thenReturn(pk);

        final Assignments assignments = update("table").with();
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getCQL3ComponentNames()).thenReturn(asList("id", "name"));
        when(pm.structure().isStaticColumn()).thenReturn(false);
        when(meta.forTranscoding().encodeToComponents(pk, false)).thenReturn(Arrays.<Object>asList(10L, "DuyHai"));

        //When
        final Pair<Where, Object[]> actual = view.generateWhereClauseForUpdate(entity, pm, assignments);

        //Then
        assertThat(actual.left.getQueryString()).isEqualTo("UPDATE table WHERE id=10 AND name=?;");
        assertThat(actual.right).containsOnly(10L, "DuyHai");
    }

    @Test
    public void should_generate_where_clause_for_update_with_simple_id() throws Exception {
        //Given
        Object entity = new Object();
        PropertyMeta pm = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.forValues().getPrimaryKey(entity)).thenReturn(10L);

        final Assignments assignments = update("table").with();
        when(meta.structure().isEmbeddedId()).thenReturn(false);
        when(meta.forTranscoding().encodeToCassandra(10L)).thenReturn(10L);
        when(meta.getCQL3ColumnName()).thenReturn("id");

        //When
        final Pair<Where, Object[]> actual = view.generateWhereClauseForUpdate(entity, pm, assignments);

        //Then
        assertThat(actual.left.getQueryString()).isEqualTo("UPDATE table WHERE id=10;");
        assertThat(actual.right).containsOnly(10L);
    }

    @Test
    public void should_prepare_select_fields_for_embedded_id() throws Exception {
        //Given
        final Selection select = select();
        when(meta.structure().isEmbeddedId()).thenReturn(true);
        when(embeddedIdProperties.getCQL3ComponentNames()).thenReturn(asList("id", "name"));

        //When
        final Selection actual = view.prepareSelectField(select);

        //Then
        assertThat(actual.from("table").getQueryString()).isEqualTo("SELECT id,name FROM table;");
    }

    @Test
    public void should_prepare_select_fields_for_id() throws Exception {
        //Given
        final Selection select = select();
        when(meta.structure().isEmbeddedId()).thenReturn(false);
        when(meta.getCQL3ColumnName()).thenReturn("id");

        //When
        final Selection actual = view.prepareSelectField(select);

        //Then
        assertThat(actual.from("table").getQueryString()).isEqualTo("SELECT id FROM table;");
    }

    @Test
    public void should_generate_update_for_remove_all() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForRemoveAll(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=:names;");
    }

    @Test
    public void should_generate_update_for_added_elements() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForAddedElements(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=names+:names;");
    }

    @Test
    public void should_generate_update_for_removed_elements() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForRemovedElements(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=names-:names;");
    }

    @Test
    public void should_generate_update_for_appended_elements() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForAppendedElements(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=names+:names;");
    }

    @Test
    public void should_generate_update_for_prepended_elements() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForPrependedElements(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=:names+names;");
    }

    @Test
    public void should_generate_update_for_remove_list_elements() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForRemoveListElements(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=names-:names;");
    }

    @Test
    public void should_generate_update_for_added_entries() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForAddedEntries(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names=names+:names;");
    }

    @Test
    public void should_generate_update_for_removed_key() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForRemovedKey(conditions);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names[:key]=:nullValue;");
    }

    @Test
    public void should_generate_update_for_set_at_index() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForSetAtIndexElement(conditions, 2, "DuyHai");

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names[2]=?;");
    }

    @Test
    public void should_generate_update_for_remove_at_index() throws Exception {
        //Given
        final Conditions conditions = update("table").onlyIf();
        when(meta.getCQL3ColumnName()).thenReturn("names");

        //When
        final Assignments actual = view.generateUpdateForRemovedAtIndexElement(conditions, 2);

        //Then
        assertThat(actual.getQueryString()).isEqualTo("UPDATE table SET names[2]=null;");
    }
}