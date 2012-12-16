package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
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
import fr.doan.achilles.wrapper.EntrySetWrapper;

@RunWith(MockitoJUnitRunner.class)
public class EntrySetWrapperBuilderTest
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

		EntrySetWrapper<Integer, String> entrySetWrapper = EntrySetWrapperBuilder
				.builder(map.entrySet()).dirtyMap(dirtyMap).setter(setter)
				.propertyMeta(propertyMeta).build();

		assertThat(entrySetWrapper.getDirtyMap()).isSameAs(dirtyMap);

		Entry<Integer, String> entry = map.entrySet().iterator().next();
		entrySetWrapper.remove(entry);

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
