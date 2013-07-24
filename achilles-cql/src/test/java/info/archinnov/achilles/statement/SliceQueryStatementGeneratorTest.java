package info.archinnov.achilles.statement;

import static info.archinnov.achilles.type.BoundingMode.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * SliceQueryStatementGeneratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryStatementGeneratorTest {

    private SliceQueryStatementGenerator generator = new SliceQueryStatementGenerator();

    @Mock
    private CQLSliceQuery<ClusteredEntity> sliceQuery;

    private UUID uuid1 = new UUID(10, 11);

    private List<String> componentNames = Arrays.asList("id", "a", "b", "c");

    @Before
    public void setUp()
    {
        when(sliceQuery.getComponentNames()).thenReturn(componentNames);
    }

    @Test
    public void should_generate_where_clause_when_same_number_of_components() throws Exception
    {
        when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
        when(sliceQuery.getLastStartComponent()).thenReturn(1);
        when(sliceQuery.getLastEndComponent()).thenReturn(2);

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
        Statement statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>=1 AND c<=2;");

        when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>1 AND c<2;");

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>=1 AND c<2;");

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>1 AND c<=2;");

    }

    @Test
    public void should_generate_where_clause_when_more_components_for_start() throws Exception
    {
        when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
        when(sliceQuery.getLastStartComponent()).thenReturn(1);
        when(sliceQuery.getLastEndComponent()).thenReturn(null);

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
        Statement statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>=1;");

        when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>1;");

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>=1;");

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>1;");

    }

    @Test
    public void should_generate_where_clause_when_more_components_for_end() throws Exception
    {
        when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
        when(sliceQuery.getLastStartComponent()).thenReturn(null);
        when(sliceQuery.getLastEndComponent()).thenReturn(2);

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
        Statement statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<=2;");

        when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<2;");

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<2;");

        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
        statement = generator.generateWhereClauseForSliceQuery(sliceQuery, buildFakeSelect());
        assertThat(statement.getQueryString()).isEqualTo(
                "SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<=2;");

    }

    private Select buildFakeSelect() {
        Select select = QueryBuilder.select("test").from("table");
        return select;
    }
}
