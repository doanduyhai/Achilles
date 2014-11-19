package info.archinnov.achilles.internal.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class StatementHelperTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private BoundStatement boundStatement;

    @Test
    public void should_get_query_string_from_regular_statement() throws Exception {
        //Given
        final RegularStatement statement = select().from("test").where(eq("id", bindMarker("id")));

        //When

        //Then
        assertThat(StatementHelper.maybeGetQueryString(statement)).isEqualTo("SELECT * FROM test WHERE id=:id;");
    }

    @Test
    public void should_get_query_string_from_bouund_statement() throws Exception {
        //Given
        when(boundStatement.preparedStatement().getQueryString()).thenReturn("test");

        //When

        //Then
        assertThat(StatementHelper.maybeGetQueryString(boundStatement)).isEqualTo("test");
    }

    @Test
    public void should_determine_whether_columns_are_all_static() throws Exception {
        //Given
        PropertyMeta meta1 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta meta2 = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta1.structure().isStaticColumn()).thenReturn(true);
        when(meta2.structure().isStaticColumn()).thenReturn(true);

        //When
        final boolean actual = StatementHelper.hasOnlyStaticColumns(Arrays.asList(meta1, meta2));

        //Then
        assertThat(actual).isTrue();
    }
}