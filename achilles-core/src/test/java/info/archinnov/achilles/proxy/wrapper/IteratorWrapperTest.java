package info.archinnov.achilles.proxy.wrapper;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
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
 * AchillesIteratorWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class IteratorWrapperTest
{
    @Mock
    private Map<Method, PropertyMeta> dirtyMap;

    private Method setter;

    @Mock
    private PropertyMeta propertyMeta;

    @Before
    public void setUp() throws Exception
    {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
    }

    @Test
    public void should_mark_dirty_on_element_remove() throws Exception
    {

        List<Object> list = new ArrayList<Object>();
        list.add(1);
        list.add(2);

        IteratorWrapper wrapper = new IteratorWrapper(list.iterator());
        wrapper.setDirtyMap(dirtyMap);
        wrapper.setSetter(setter);
        wrapper.setPropertyMeta(propertyMeta);

        when(propertyMeta.type()).thenReturn(PropertyType.LIST);

        wrapper.next();
        wrapper.remove();

        verify(dirtyMap).put(setter, propertyMeta);
    }
}
