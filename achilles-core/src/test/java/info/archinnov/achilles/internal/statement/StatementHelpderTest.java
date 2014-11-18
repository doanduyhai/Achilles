package info.archinnov.achilles.internal.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StatementHelpderTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private BoundStatement boundStatement;

    @Test
    public void should_get_query_string_from_regular_statement() throws Exception {
        //Given
        final RegularStatement statement = select().from("test").where(eq("id", bindMarker("id")));

        //When

        //Then
        assertThat(StatementHelpder.maybeGetQueryString(statement)).isEqualTo("SELECT * FROM test WHERE id=:id;");
    }

    @Test
    public void should_get_query_string_from_bouund_statement() throws Exception {
        //Given
        when(boundStatement.preparedStatement().getQueryString()).thenReturn("test");

        //When

        //Then
        assertThat(StatementHelpder.maybeGetQueryString(boundStatement)).isEqualTo("test");
    }
}