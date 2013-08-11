package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesMapEntryWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MapEntryWrapperTest
{
    @Mock
    private Map<Method, PropertyMeta> dirtyMap;

    @Mock
    private EntityProxifier<PersistenceContext> proxifier;

    private Method setter;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private PersistenceContext context;

    @Mock
    private PersistenceContext joinContext;

    @Before
    public void setUp() throws Exception
    {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
        when(propertyMeta.type()).thenReturn(PropertyType.MAP);
    }

    @Test
    public void should_mark_dirty_on_value_set() throws Exception
    {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "FR");
        map.put(2, "Paris");
        map.put(3, "75014");
        Entry<Integer, String> mapEntry = map.entrySet().iterator().next();

        MapEntryWrapper mapEntryWrapper = new MapEntryWrapper((Entry) mapEntry);
        mapEntryWrapper.setProxifier(proxifier);
        mapEntryWrapper.setDirtyMap(dirtyMap);
        mapEntryWrapper.setSetter(setter);
        mapEntryWrapper.setPropertyMeta(propertyMeta);
        when(proxifier.unwrap("TEST")).thenReturn("TEST");
        mapEntryWrapper.setValue("TEST");

        verify(dirtyMap).put(setter, propertyMeta);

    }

    @Test
    public void should_equal() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);
        wrapper1.setProxifier(proxifier);
        wrapper1.setPropertyMeta(propertyMeta);
        wrapper2.setProxifier(proxifier);
        wrapper2.setPropertyMeta(propertyMeta);

        when(proxifier.unwrap("csdf")).thenReturn("csdf");

        assertThat(wrapper1.equals(wrapper2)).isTrue();
    }

    @Test
    public void should_not_equal_when_values_differ() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "df");

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        wrapper1.setProxifier(proxifier);
        wrapper1.setPropertyMeta(propertyMeta);
        wrapper2.setProxifier(proxifier);
        wrapper2.setPropertyMeta(propertyMeta);

        when(proxifier.unwrap("csdf")).thenReturn("csdf");
        when(proxifier.unwrap("df")).thenReturn("df");

        assertThat(wrapper1.equals(wrapper2)).isFalse();
    }

    @Test
    public void should_not_equal_when_one_value_null() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "csdf");
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        wrapper1.setProxifier(proxifier);
        wrapper1.setPropertyMeta(propertyMeta);
        wrapper2.setProxifier(proxifier);
        wrapper2.setPropertyMeta(propertyMeta);

        when(proxifier.unwrap((Object) null)).thenReturn(null);
        assertThat(wrapper1.equals(wrapper2)).isFalse();
    }

    @Test
    public void should_equal_compare_key_when_both_values_null() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, null);
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        wrapper1.setProxifier(proxifier);
        wrapper1.setPropertyMeta(propertyMeta);
        wrapper2.setProxifier(proxifier);
        wrapper2.setPropertyMeta(propertyMeta);

        when(proxifier.unwrap((Object) null)).thenReturn(null);
        assertThat(wrapper1.equals(wrapper2)).isTrue();
    }

    @Test
    public void should_not_equal_when_keys_differ() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, null);
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        wrapper1.setProxifier(proxifier);
        wrapper1.setPropertyMeta(propertyMeta);
        wrapper2.setProxifier(proxifier);
        wrapper2.setPropertyMeta(propertyMeta);

        when(proxifier.unwrap((Object) null)).thenReturn(null);
        assertThat(wrapper1.equals(wrapper2)).isFalse();
    }

    @Test
    public void should_same_hashcode_when_same_keys_and_values() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        assertThat(wrapper1.hashCode()).isEqualTo(wrapper2.hashCode());
    }

    @Test
    public void should_different_hashcode_when_values_differ() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, null);

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        assertThat(wrapper1.hashCode()).isNotEqualTo(wrapper2.hashCode());
    }

    @Test
    public void should_different_hashcode_when_keys_differ() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, "abc");
        Map.Entry<Integer, String> entry2 = new AbstractMap.SimpleEntry<Integer, String>(4, "abc");

        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);
        MapEntryWrapper wrapper2 = new MapEntryWrapper((Entry) entry2);

        assertThat(wrapper1.hashCode()).isNotEqualTo(wrapper2.hashCode());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_get_join_value() throws Exception
    {
        Map.Entry<Integer, String> entry1 = new AbstractMap.SimpleEntry<Integer, String>(1, "abc");
        MapEntryWrapper wrapper1 = new MapEntryWrapper((Entry) entry1);

        EntityMeta joinMeta = new EntityMeta();

        wrapper1.setProxifier(proxifier);
        wrapper1.setPropertyMeta(propertyMeta);
        wrapper1.setContext(context);
        when(propertyMeta.type()).thenReturn(PropertyType.JOIN_MAP);
        when(propertyMeta.joinMeta()).thenReturn(joinMeta);
        when(context.createContextForJoin(joinMeta, "abc")).thenReturn(joinContext);

        when(proxifier.buildProxy("abc", joinContext)).thenReturn("def");

        assertThat(wrapper1.getValue()).isSameAs("def");
    }
}
