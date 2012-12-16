package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.ListIteratorWrapper;

@RunWith(MockitoJUnitRunner.class)
public class ListIteratorWrapperBuilderTest
{
	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Void, String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
	}

	@Test
	public void should_build() throws Exception
	{
		List<String> target = new ArrayList<String>();
		target.add("a");

		ListIteratorWrapper<String> listIteratorWrapper = ListIteratorWrapperBuilder
				.builder(target.listIterator()).dirtyMap(dirtyMap).setter(setter)
				.propertyMeta(propertyMeta).build();

		assertThat(listIteratorWrapper.getDirtyMap()).isSameAs(dirtyMap);

		listIteratorWrapper.add("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
