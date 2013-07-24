package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftCounterClusteredEntityIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftCounterClusteredEntityIteratorTest
{
    private ThriftCounterClusteredEntityIterator<BeanWithClusteredId> iterator;

    private Class<BeanWithClusteredId> entityClass = BeanWithClusteredId.class;

    @Mock
    private ThriftCounterSliceIterator<Object> sliceIterator;

    @Mock
    private ThriftPersistenceContext context;

    @Mock
    private ThriftCompositeTransformer transformer;

    @Mock
    private HCounterColumn<Composite> hColumn;

    private BeanWithClusteredId entity = new BeanWithClusteredId();

    @Before
    public void setUp()
    {
        iterator = new ThriftCounterClusteredEntityIterator<BeanWithClusteredId>(entityClass,
                sliceIterator, context);
        iterator = spy(iterator);

        Whitebox.setInternalState(iterator, ThriftCompositeTransformer.class, transformer);
    }

    @Test
    public void should_get_next() throws Exception
    {
        when(sliceIterator.next()).thenReturn(hColumn);
        when(transformer.buildCounterClusteredEntity(entityClass, context, hColumn)).thenReturn(
                entity);
        doReturn(entity).when(iterator).proxifyClusteredEntity(entity);
        assertThat(iterator.next()).isSameAs(entity);
    }

}
