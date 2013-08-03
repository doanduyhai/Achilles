package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Query;
import com.google.common.base.Optional;

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
        context = new CQLBatchingFlushContext(daoContext,
                Optional.fromNullable(ConsistencyLevel.EACH_QUORUM),
                Optional.fromNullable(ConsistencyLevel.LOCAL_QUORUM),
                Optional.fromNullable(11));
    }

    @Test
    public void should_start_batch() throws Exception
    {
        context.boundStatements.add(bs);
        context.readLevelO = Optional.fromNullable(ConsistencyLevel.ALL);
        context.writeLevelO = Optional.fromNullable(ConsistencyLevel.ALL);
        context.ttlO = Optional.fromNullable(10);

        context.startBatch();

        assertThat(context.boundStatements).isEmpty();
        assertThat(context.readLevelO.isPresent()).isFalse();
        assertThat(context.writeLevelO.isPresent()).isFalse();
        assertThat(context.ttlO.isPresent()).isFalse();
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
        context.readLevelO = Optional.fromNullable(ConsistencyLevel.ALL);
        context.writeLevelO = Optional.fromNullable(ConsistencyLevel.ALL);
        context.ttlO = Optional.fromNullable(10);

        context.endBatch();

        assertThat(context.boundStatements).isEmpty();
        assertThat(context.readLevelO.isPresent()).isFalse();
        assertThat(context.writeLevelO.isPresent()).isFalse();
        assertThat(context.ttlO.isPresent()).isFalse();
    }

    @Test
    public void should_get_type() throws Exception
    {
        assertThat(context.type()).isSameAs(FlushType.BATCH);
    }

    @Test
    public void should_duplicate() throws Exception
    {
        context.boundStatements.add(bs);

        CQLBatchingFlushContext duplicate = context.duplicateWithoutTtl();

        assertThat(duplicate.boundStatements).containsOnly(bs);
        assertThat(duplicate.readLevelO.get()).isSameAs(ConsistencyLevel.EACH_QUORUM);
        assertThat(duplicate.writeLevelO.get()).isSameAs(ConsistencyLevel.LOCAL_QUORUM);
        assertThat(duplicate.ttlO.isPresent()).isFalse();
    }
}
