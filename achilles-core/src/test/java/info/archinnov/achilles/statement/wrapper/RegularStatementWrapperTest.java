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

@RunWith(MockitoJUnitRunner.class)
public class RegularStatementWrapperTest {

    private RegularStatementWrapper wrapper;

    @Mock
    private RegularStatement rs;

    @Mock
    private Session session;

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(rs,new Object[]{1}, ConsistencyLevel.ONE);

        //When
        wrapper.execute(session);

        //Then
        verify(session).execute(rs);
    }

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(rs,new Object[]{1}, ConsistencyLevel.ONE);

        //When
        final RegularStatement expectedRs = wrapper.getStatement();

        //Then
        assertThat(expectedRs).isSameAs(rs);
    }
}
