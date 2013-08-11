package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.proxy.wrapper.IteratorWrapper;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesIteratorWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class IteratorWrapperBuilderTest
{
    @Mock
    private Map<Method, PropertyMeta> dirtyMap;

    private Method setter;

    @Mock
    private EntityProxifier<PersistenceContext> proxifier;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private PersistenceContext context;

    @Before
    public void setUp() throws Exception
    {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
    }

    @Test
    public void should_build() throws Exception
    {
        List<Object> target = new ArrayList<Object>();
        target.add("a");

        Iterator<Object> iterator = target.iterator();
        IteratorWrapper wrapper = IteratorWrapperBuilder //
                .builder(context, iterator)
                .dirtyMap(dirtyMap)
                .setter(setter)
                .propertyMeta(propertyMeta)
                .proxifier(proxifier)
                .build();

        assertThat(Whitebox.getInternalState(wrapper, "target")).isSameAs(iterator);
        assertThat(wrapper.getDirtyMap()).isSameAs(dirtyMap);
        assertThat(Whitebox.getInternalState(wrapper, "setter")).isSameAs(setter);
        assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
        assertThat(Whitebox.getInternalState(wrapper, "proxifier")).isSameAs(proxifier);
        assertThat(Whitebox.getInternalState(wrapper, "context")).isSameAs(context);

    }
}
