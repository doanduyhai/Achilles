package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.ListWrapper;
import info.archinnov.achilles.wrapper.builder.ListWrapperBuilder;

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


/**
 * ListWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ListWrapperBuilderTest
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
		ListWrapper<String> listWrapper = ListWrapperBuilder.builder(target).dirtyMap(dirtyMap)
				.setter(setter).propertyMeta(propertyMeta).build();

		assertThat(listWrapper.getTarget()).isSameAs(target);
		assertThat(listWrapper.getDirtyMap()).isSameAs(dirtyMap);

		listWrapper.add("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
