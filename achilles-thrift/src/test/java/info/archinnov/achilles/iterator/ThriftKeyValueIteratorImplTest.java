package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.type.KeyValue;
import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import parser.entity.CorrectCompoundKey;

/**
 * ThriftKeyValueIteratorImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftKeyValueIteratorImplTest
{

    @InjectMocks
    private ThriftKeyValueIteratorImpl<CorrectCompoundKey, String> iterator;

    @Mock
    private ThriftSliceIterator<CorrectCompoundKey, String> thriftSliceIterator;

    @Mock
    private List<Method> componentSetters;

    @Mock
    private PropertyMeta<CorrectCompoundKey, String> multiKeyWideMapMeta;

    @Mock
    private ThriftKeyValueFactory factory;

    @Mock
    private ThriftPersistenceContext context;

    @Before
    public void setUp()
    {
        Whitebox.setInternalState(iterator, "factory", factory);
        when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectCompoundKey.class);
    }

    @Test
    public void should_has_next() throws Exception
    {
        when(thriftSliceIterator.hasNext()).thenReturn(true, true, false);

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.hasNext()).isFalse();

    }

    @Test
    public void should_give_next_keyvalue() throws Exception
    {
        KeyValue<CorrectCompoundKey, String> keyValue = mock(KeyValue.class);
        HColumn<Composite, String> hColumn = mock(HColumn.class);

        when(thriftSliceIterator.hasNext()).thenReturn(true, false);
        when(thriftSliceIterator.next()).thenReturn(hColumn);
        when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectCompoundKey.class);
        when(multiKeyWideMapMeta.getComponentSetters()).thenReturn(componentSetters);

        when(factory.createKeyValue(context, multiKeyWideMapMeta, hColumn)).thenReturn(keyValue);

        KeyValue<CorrectCompoundKey, String> result = iterator.next();

        assertThat(result).isSameAs(keyValue);
    }

    @Test
    public void should_give_next_key() throws Exception
    {
        CorrectCompoundKey key = mock(CorrectCompoundKey.class);
        HColumn<Composite, String> hColumn = mock(HColumn.class);

        when(thriftSliceIterator.hasNext()).thenReturn(true, false);
        when(thriftSliceIterator.next()).thenReturn(hColumn);
        when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectCompoundKey.class);
        when(multiKeyWideMapMeta.getComponentSetters()).thenReturn(componentSetters);

        when(factory.createKey(multiKeyWideMapMeta, hColumn)).thenReturn(key);

        CorrectCompoundKey result = iterator.nextKey();

        assertThat(result).isSameAs(key);
    }

    @Test
    public void should_give_next_value() throws Exception
    {
        String value = "value";
        HColumn<Composite, String> hColumn = mock(HColumn.class);

        when(thriftSliceIterator.hasNext()).thenReturn(true, false);
        when(thriftSliceIterator.next()).thenReturn(hColumn);
        when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectCompoundKey.class);
        when(multiKeyWideMapMeta.getComponentSetters()).thenReturn(componentSetters);

        when(factory.createValue(context, multiKeyWideMapMeta, hColumn)).thenReturn(value);

        String result = iterator.nextValue();

        assertThat(result).isSameAs(value);
    }

    @Test
    public void should_give_next_ttl() throws Exception
    {
        Integer ttl = 123;
        HColumn<Composite, String> hColumn = mock(HColumn.class);

        when(thriftSliceIterator.hasNext()).thenReturn(true, false);
        when(thriftSliceIterator.next()).thenReturn(hColumn);
        when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectCompoundKey.class);
        when(multiKeyWideMapMeta.getComponentSetters()).thenReturn(componentSetters);

        when(factory.createTtl(hColumn)).thenReturn(ttl);

        Integer result = iterator.nextTtl();

        assertThat(result).isSameAs(ttl);
    }

    @Test(expected = NoSuchElementException.class)
    public void should_exception_when_no_more_element() throws Exception
    {
        when(thriftSliceIterator.hasNext()).thenReturn(false);
        iterator.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_remove_called() throws Exception
    {
        iterator.remove();
    }
}
