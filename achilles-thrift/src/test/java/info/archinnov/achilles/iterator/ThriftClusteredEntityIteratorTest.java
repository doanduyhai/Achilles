package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.lang.reflect.Method;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftClusteredEntityIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftClusteredEntityIteratorTest
{
    private ThriftClusteredEntityIterator<BeanWithClusteredId> iterator;

    private Class<BeanWithClusteredId> entityClass = BeanWithClusteredId.class;

    @Mock
    private ThriftAbstractSliceIterator<HColumn<Composite, Object>> sliceIterator;

    @Mock
    private ThriftPersistenceContext context;

    @Mock
    private ThriftCompositeTransformer transformer;

    @Mock
    private HColumn<Composite, Object> hColumn;

    private BeanWithClusteredId entity = new BeanWithClusteredId();

    @Before
    public void setUp()
    {
        iterator = new ThriftClusteredEntityIterator<BeanWithClusteredId>(entityClass,
                sliceIterator, context);
        iterator = spy(iterator);
        Whitebox.setInternalState(iterator, "transformer", transformer);
    }

    @Test
    public void should_get_next() throws Exception
    {
        Method idGetter = BeanWithClusteredId.class.getDeclaredMethod("getId");

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .build();
        idMeta.setGetter(idGetter);

        when(sliceIterator.next()).thenReturn(hColumn);
        when(transformer.buildClusteredEntity(entityClass, context, hColumn)).thenReturn(entity);
        doReturn(entity).when(iterator).proxifyClusteredEntity(entity);
        assertThat(iterator.next()).isSameAs(entity);
    }

}
