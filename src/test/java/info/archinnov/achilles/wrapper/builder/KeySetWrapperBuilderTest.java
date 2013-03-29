package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.ValueCollectionWrapper;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * KeySetWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeySetWrapperBuilderTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private PropertyMeta<Void, String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFollowers", Set.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_build() throws Exception
	{
		Map<Integer, String> targetMap = new HashMap<Integer, String>();
		targetMap.put(1, "FR");
		targetMap.put(2, "Paris");
		targetMap.put(3, "75014");

		ValueCollectionWrapper<String> wrapper = ValueCollectionWrapperBuilder
				.builder(targetMap.values()) //
				.dirtyMap(dirtyMap) //
				.setter(setter) //
				.propertyMeta((PropertyMeta) propertyMeta) //
				.helper(introspector) //
				.build();

		assertThat(wrapper.getTarget()).isSameAs(targetMap.values());
		assertThat(wrapper.getDirtyMap()).isSameAs(dirtyMap);
		assertThat(Whitebox.getInternalState(wrapper, "setter")).isSameAs(setter);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "helper")).isSameAs(introspector);

	}
}
