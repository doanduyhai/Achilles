package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesEntryIteratorWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntryIteratorWrapperTest
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
    public void should_return_true_on_hasNext() throws Exception
    {
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, "FR");
        EntryIteratorWrapper wrapper = new EntryIteratorWrapper(map.entrySet().iterator());

        assertThat(wrapper.hasNext()).isTrue();
    }

    @Test
    public void should_mark_dirty_on_element_remove() throws Exception
    {

        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, "FR");
        map.put(2, "Paris");
        map.put(3, "75014");

        EntryIteratorWrapper wrapper = new EntryIteratorWrapper(map.entrySet().iterator());
        wrapper.setDirtyMap(dirtyMap);
        wrapper.setSetter(setter);
        wrapper.setPropertyMeta(propertyMeta);

        wrapper.next();
        wrapper.remove();

        verify(dirtyMap).put(setter, propertyMeta);
    }

}
