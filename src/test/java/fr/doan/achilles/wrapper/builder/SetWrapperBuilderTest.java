package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.SetWrapper;

/**
 * SetWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class SetWrapperBuilderTest
{

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<Void, String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFollowers", Set.class);
	}

	@Test
	public void should_build() throws Exception
	{
		Set<String> target = new HashSet<String>();
		SetWrapper<String> setWrapper = SetWrapperBuilder.builder(target).dirtyMap(dirtyMap)
				.setter(setter).propertyMeta(propertyMeta).build();

		assertThat(setWrapper.getTarget()).isSameAs(target);
		assertThat(setWrapper.getDirtyMap()).isSameAs(dirtyMap);

		setWrapper.add("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
