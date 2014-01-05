package info.archinnov.achilles.internal.statement.wrapper;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

@RunWith(MockitoJUnitRunner.class)
public class BoundStatementWrapperTest {

    private BoundStatementWrapper wrapper;
    
    @Mock
    private BoundStatement bs;

    @Mock
    private PreparedStatement ps;

    @Mock
    private Session session;

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(bs,new Object[]{1}, ConsistencyLevel.ONE);
        when(bs.preparedStatement()).thenReturn(ps);
        when(ps.getQueryString()).thenReturn("SELECT");

        //When
        wrapper.execute(session);

        //Then
        verify(session).execute(bs);
    }

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(bs,new Object[]{1}, ConsistencyLevel.ONE);

        //When
        final BoundStatement expectedBs = wrapper.getStatement();

        //Then
        assertThat(expectedBs).isSameAs(bs);
    }
}
