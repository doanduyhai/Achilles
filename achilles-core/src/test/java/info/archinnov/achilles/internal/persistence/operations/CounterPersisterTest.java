package info.archinnov.achilles.internal.persistence.operations;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

@RunWith(MockitoJUnitRunner.class)
public class CounterPersisterTest {

    private CounterPersister persister = new CounterPersister();

    @Mock
    private PersistenceContext context;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta counterMeta;

    @Mock
    private PropertyMeta counterMeta2;

    @Mock
    private PropertyMeta counterMeta3;

    private Object entity = new Object();

    @Before
    public void setUp() {
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getEntity()).thenReturn(entity);
    }

    @Test
    public void should_persist_counters() throws Exception {
        //Given
        final long delta = 10L;
        final Counter counter = CounterBuilder.incr(delta);
        when(counterMeta.getValueFromField(entity)).thenReturn(counter);

        //When
        persister.persistCounters(context, asList(counterMeta));

        //Then
        verify(context).bindForSimpleCounterIncrement(counterMeta, delta);

    }

    @Test
    public void should_not_persist_counters_if_null_counter() throws Exception {
        //When
        persister.persistCounters(context, asList(counterMeta));

        //Then
        verify(context,never()).bindForSimpleCounterIncrement(eq(counterMeta), anyLong());
    }

    @Test
    public void should_not_persist_counters_if_no_delta() throws Exception {
        //Given
        final long delta = 0L;
        final Counter counter = CounterBuilder.incr(delta);
        when(counterMeta.getValueFromField(entity)).thenReturn(counter);

        //When
        persister.persistCounters(context, asList(counterMeta));

        //Then
        verify(context,never()).bindForSimpleCounterIncrement(counterMeta, delta);
    }

    @Test
    public void should_persist_clustered_counters() throws Exception {
        //Given
        final long delta = 10L;
        final Counter counter = CounterBuilder.incr(delta);
        when(counterMeta.getValueFromField(entity)).thenReturn(counter);
        when(counterMeta3.getValueFromField(entity)).thenReturn(CounterBuilder.incr(0L));
        when(context.getAllCountersMeta()).thenReturn(asList(counterMeta,counterMeta2,counterMeta3));

        //When
        persister.persistClusteredCounters(context);

        //Then
        verify(context).pushClusteredCounterIncrementStatement(counterMeta,delta);
    }

    @Test(expected = IllegalStateException.class)
    public void should_exception_when_persisting_null_clustered_counters() throws Exception {
        //Given
        when(context.getAllCountersMeta()).thenReturn(asList(counterMeta,counterMeta));

        //When
        persister.persistClusteredCounters(context);
    }

    @Test
    public void should_remove_related_counters() throws Exception {
        //Given
        when(meta.getAllCounterMetas()).thenReturn(asList(counterMeta,counterMeta2));

        //When
        persister.removeRelatedCounters(context);

        //Then
        verify(context).bindForSimpleCounterRemoval(counterMeta);
        verify(context).bindForSimpleCounterRemoval(counterMeta2);
    }
    
}
