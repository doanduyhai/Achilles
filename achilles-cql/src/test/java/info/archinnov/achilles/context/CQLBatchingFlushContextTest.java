package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Query;

/**
 * CQLBatchingFlushContextTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CQLBatchingFlushContextTest {

    private CQLBatchingFlushContext context;

    @Mock
    private CQLDaoContext daoContext;

    @Mock
    private BoundStatementWrapper bsWrapper;

    @Mock
    private Query query;

    @Before
    public void setUp()
    {
        context = new CQLBatchingFlushContext(daoContext, EACH_QUORUM);
    }

    @Test
    public void should_start_batch() throws Exception
    {
        context.boundStatementWrappers.add(bsWrapper);

        context.startBatch();

        assertThat(context.boundStatementWrappers).isEmpty();
        assertThat(context.consistencyLevel).isNull();
    }

    @Test
    public void should_do_nothing_when_flush_is_called() throws Exception
    {
        context.boundStatementWrappers.add(bsWrapper);

        context.flush();

        assertThat(context.boundStatementWrappers).containsExactly(bsWrapper);
    }

    @Test
    public void should_end_batch() throws Exception
    {
        context.boundStatementWrappers.add(bsWrapper);

        context.endBatch();

        assertThat(context.boundStatementWrappers).isEmpty();
        assertThat(context.consistencyLevel).isNull();
    }

    @Test
    public void should_get_type() throws Exception
    {
        assertThat(context.type()).isSameAs(FlushType.BATCH);
    }

    @Test
    public void should_duplicate_without_ttl() throws Exception
    {
        context.boundStatementWrappers.add(bsWrapper);

        CQLBatchingFlushContext duplicate = context.duplicate();

        assertThat(duplicate.boundStatementWrappers).containsOnly(bsWrapper);
        assertThat(duplicate.consistencyLevel).isSameAs(EACH_QUORUM);
    }
}
