package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.FlushContext.FlushType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
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
    private BoundStatement bs;

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
        context.boundStatements.add(bs);

        context.startBatch();

        assertThat(context.boundStatements).isEmpty();
        assertThat(context.consistencyLevel).isNull();
    }

    @Test
    public void should_do_nothing_when_flush_is_called() throws Exception
    {
        context.boundStatements.add(bs);

        context.flush();

        assertThat(context.boundStatements).containsExactly(bs);
    }

    @Test
    public void should_end_batch() throws Exception
    {
        context.boundStatements.add(bs);

        context.endBatch();

        assertThat(context.boundStatements).isEmpty();
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
        context.boundStatements.add(bs);

        CQLBatchingFlushContext duplicate = context.duplicate();

        assertThat(duplicate.boundStatements).containsOnly(bs);
        assertThat(duplicate.consistencyLevel).isSameAs(EACH_QUORUM);
    }
}
