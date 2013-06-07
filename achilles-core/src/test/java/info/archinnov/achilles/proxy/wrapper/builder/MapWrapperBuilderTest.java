package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.proxy.wrapper.MapWrapper;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MapWrapperBuilderTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private AchillesPersistenceContext context;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
	}

	@Test
	public void should_build() throws Exception
	{
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		MapWrapper<Integer, String> wrapper = MapWrapperBuilder //
				.builder(context, map)
				.dirtyMap(dirtyMap)
				.setter(setter)
				.propertyMeta(propertyMeta)
				.proxifier(proxifier)
				.build();

		assertThat(wrapper.getTarget()).isSameAs(map);
		assertThat(wrapper.getDirtyMap()).isSameAs(dirtyMap);
		assertThat(Whitebox.getInternalState(wrapper, "setter")).isSameAs(setter);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "proxifier")).isSameAs(proxifier);
		assertThat(Whitebox.getInternalState(wrapper, "context")).isSameAs(context);

	}
}
