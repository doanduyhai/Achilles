package fr.doan.achilles.wrapper.builder;

import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.MapEntryWrapper;

@RunWith(MockitoJUnitRunner.class)
public class MapEntryWrapperBuilderTest
{

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

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
		Entry<Integer, String> mapEntry = map.entrySet().iterator().next();

		MapEntryWrapper<Integer, String> wrapper = MapEntryWrapperBuilder.builder(mapEntry)
				.dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();

		wrapper.setValue("sdfsdf");

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
