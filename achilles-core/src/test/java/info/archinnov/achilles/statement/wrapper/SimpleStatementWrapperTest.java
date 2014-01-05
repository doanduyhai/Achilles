package info.archinnov.achilles.statement.wrapper;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

@RunWith(MockitoJUnitRunner.class)
public class SimpleStatementWrapperTest {

    private final Object[] values = new Object[]{1};

    private SimpleStatementWrapper wrapper;

    @Mock
    private Session session;

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new SimpleStatementWrapper("SELECT", values);

        //When
        wrapper.execute(session);

        //Then
        verify(session).execute("SELECT",values);
    }

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new SimpleStatementWrapper("SELECT",values);

        //When
        final SimpleStatement simpleStatement = wrapper.getStatement();

        //Then
        assertThat(simpleStatement.getQueryString()).isEqualTo("SELECT");
    }
}
