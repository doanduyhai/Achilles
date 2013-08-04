package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.base.Optional;

/**
 * CQLCounterWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLCounterWrapperTest {

    private CQLCounterWrapper wrapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CQLPersistenceContext context;

    @Mock
    private PropertyMeta<?, ?> counterMeta;

    @Before
    public void setUp()
    {
        when(counterMeta.getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);
        when(counterMeta.getWriteConsistencyLevel()).thenReturn(EACH_QUORUM);

        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(ONE));
        when(context.getWriteConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(ALL));

    }

    @Test
    public void should_get_simple_counter_with_default_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        when(context.getReadConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
        when(context.getSimpleCounter(counterMeta, LOCAL_QUORUM)).thenReturn(counterValue);

        assertThat(wrapper.get()).isEqualTo(counterValue);
    }

    @Test
    public void should_get_simple_counter_with_runtime_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        when(context.getSimpleCounter(counterMeta, ONE)).thenReturn(counterValue);

        assertThat(wrapper.get()).isEqualTo(counterValue);
    }

    @Test
    public void should_get_simple_counter_with_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        when(context.getSimpleCounter(counterMeta, THREE)).thenReturn(counterValue);

        assertThat(wrapper.get(THREE)).isEqualTo(counterValue);
    }

    @Test
    public void should_get_clustered_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        when(context.getClusteredCounter(counterMeta, ONE)).thenReturn(counterValue);

        assertThat(wrapper.get()).isEqualTo(counterValue);
    }

    @Test
    public void should_get_clustered_counter_with_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        when(context.getClusteredCounter(counterMeta, TWO)).thenReturn(counterValue);

        assertThat(wrapper.get(TWO)).isEqualTo(counterValue);
    }

    @Test
    public void should_increment_simple_counter() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr();
        verify(context).incrementSimpleCounter(counterMeta, 1L, ALL);
    }

    @Test
    public void should_increment_clustered_counter() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr();
        verify(context).incrementClusteredCounter(counterMeta, 1L, ALL);
    }

    @Test
    public void should_increment_simple_counter_with_consistency() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr(EACH_QUORUM);
        verify(context).incrementSimpleCounter(counterMeta, 1L, EACH_QUORUM);
    }

    @Test
    public void should_increment_clustered_counter_with_consistency() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr(EACH_QUORUM);
        verify(context).incrementClusteredCounter(counterMeta, 1L, EACH_QUORUM);
    }

    @Test
    public void should_increment_n_simple_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr(counterValue);
        verify(context).incrementSimpleCounter(counterMeta, counterValue, ALL);
    }

    @Test
    public void should_increment_n_clustered_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr(counterValue);
        verify(context).incrementClusteredCounter(counterMeta, counterValue, ALL);
    }

    @Test
    public void should_increment_n_simple_counter_with_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr(counterValue, EACH_QUORUM);
        verify(context).incrementSimpleCounter(counterMeta, counterValue, EACH_QUORUM);
    }

    @Test
    public void should_increment_n_clustered_counter_with_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.incr(counterValue, EACH_QUORUM);
        verify(context).incrementClusteredCounter(counterMeta, counterValue, EACH_QUORUM);
    }

    @Test
    public void should_decrement_simple_counter() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr();
        verify(context).decrementSimpleCounter(counterMeta, 1L, ALL);
    }

    @Test
    public void should_decrement_clustered_counter() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr();
        verify(context).decrementClusteredCounter(counterMeta, 1L, ALL);
    }

    @Test
    public void should_decrement_simple_counter_with_consistency() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr(EACH_QUORUM);
        verify(context).decrementSimpleCounter(counterMeta, 1L, EACH_QUORUM);
    }

    @Test
    public void should_decrement_clustered_counter_with_consistency() throws Exception
    {
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr(EACH_QUORUM);
        verify(context).decrementClusteredCounter(counterMeta, 1L, EACH_QUORUM);
    }

    @Test
    public void should_decrement_n_simple_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr(counterValue);
        verify(context).decrementSimpleCounter(counterMeta, counterValue, ALL);
    }

    @Test
    public void should_decrement_n_clustered_counter() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr(counterValue);
        verify(context).decrementClusteredCounter(counterMeta, counterValue, ALL);
    }

    @Test
    public void should_decrement_n_simple_counter_with_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr(counterValue, EACH_QUORUM);
        verify(context).decrementSimpleCounter(counterMeta, counterValue, EACH_QUORUM);
    }

    @Test
    public void should_decrement_n_clustered_counter_with_consistency() throws Exception
    {
        Long counterValue = RandomUtils.nextLong();
        when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
        wrapper = new CQLCounterWrapper(context, counterMeta);

        wrapper.decr(counterValue, EACH_QUORUM);
        verify(context).decrementClusteredCounter(counterMeta, counterValue, EACH_QUORUM);
    }
}
