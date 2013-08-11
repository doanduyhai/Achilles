package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.proxy.wrapper.ValueCollectionWrapper;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesKeySetWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeySetWrapperBuilderTest
{
    @Mock
    private Map<Method, PropertyMeta> dirtyMap;

    private Method setter;

    @Mock
    private EntityProxifier<PersistenceContext> proxifier;

    @Mock
    private PersistenceContext context;

    @Mock
    private PropertyMeta propertyMeta;

    @Before
    public void setUp() throws Exception
    {
        setter = CompleteBean.class.getDeclaredMethod("setFollowers", Set.class);
    }

    @Test
    public void should_build() throws Exception
    {
        Map<Object, Object> targetMap = new HashMap<Object, Object>();
        targetMap.put(1, "FR");
        targetMap.put(2, "Paris");
        targetMap.put(3, "75014");

        ValueCollectionWrapper wrapper = ValueCollectionWrapperBuilder
                .builder(context, targetMap.values())
                .dirtyMap(dirtyMap)
                .setter(setter)
                .propertyMeta((PropertyMeta) propertyMeta)
                .proxifier(proxifier)
                .build();

        assertThat(wrapper.getTarget()).isSameAs(targetMap.values());
        assertThat(wrapper.getDirtyMap()).isSameAs(dirtyMap);
        assertThat(Whitebox.getInternalState(wrapper, "setter")).isSameAs(setter);
        assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
        assertThat(Whitebox.getInternalState(wrapper, "proxifier")).isSameAs(proxifier);
        assertThat(Whitebox.getInternalState(wrapper, "context")).isSameAs(context);

    }
}
