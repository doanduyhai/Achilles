package info.archinnov.achilles.proxy.wrapper;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesListIteratorWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ListIteratorWrapperTest
{

    @Mock
    private Map<Method, PropertyMeta> dirtyMap;

    private Method setter;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private EntityProxifier<PersistenceContext> proxifier;

    private ListIteratorWrapper wrapper;

    @Before
    public void setUp() throws Exception
    {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);

        List<Object> list = new ArrayList<Object>();
        list.add(1);
        list.add(2);

        wrapper = new ListIteratorWrapper(list.listIterator());
        wrapper.setDirtyMap(dirtyMap);
        wrapper.setSetter(setter);
        wrapper.setPropertyMeta(propertyMeta);
        wrapper.setProxifier(proxifier);

        when(propertyMeta.type()).thenReturn(PropertyType.LIST);
    }

    @Test
    public void should_mark_dirty_on_add() throws Exception
    {
        wrapper.add(3);

        verify(dirtyMap).put(setter, propertyMeta);
    }

    @Test
    public void should_mark_dirty_on_set() throws Exception
    {
        when(proxifier.unwrap(1)).thenReturn(1);
        wrapper.next();
        wrapper.set(1);

        verify(dirtyMap).put(setter, propertyMeta);
    }

    @Test
    public void should_mark_dirty_on_remove() throws Exception
    {

        wrapper.next();
        wrapper.remove();

        verify(dirtyMap).put(setter, propertyMeta);
    }
}
