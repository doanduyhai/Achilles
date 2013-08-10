package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import java.util.Arrays;
import java.util.List;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
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

    @Mock
    private Composite composite;

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
        List<Component<?>> components = Arrays.<Component<?>> asList(mock(Component.class));
        when(sliceIterator.next()).thenReturn(hColumn);
        when(hColumn.getName()).thenReturn(composite);
        when(composite.getComponents()).thenReturn(components);
        when(transformer.buildClusteredEntityWithIdOnly(entityClass, context, components)).thenReturn(
                entity);
        doReturn(entity).when(iterator).proxifyClusteredEntity(entity);
        assertThat(iterator.next()).isSameAs(entity);
    }

}
