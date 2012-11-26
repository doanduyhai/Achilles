package fr.doan.achilles.proxy.builder;

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
import fr.doan.achilles.proxy.collection.SetProxy;
import fr.doan.achilles.proxy.builder.SetProxyBuilder;

@RunWith(MockitoJUnitRunner.class)
public class SetProxyBuilderTest
{

	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

	private Method setter;

	@Mock
	private PropertyMeta<String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		setter = CompleteBean.class.getDeclaredMethod("setFollowers", Set.class);
	}

	@Test
	public void should_build() throws Exception
	{
		Set<String> target = new HashSet<String>();
		SetProxy<String> setProxy = SetProxyBuilder.builder(target).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();

		assertThat(setProxy.getTarget()).isSameAs(target);
		assertThat(setProxy.getDirtyMap()).isSameAs(dirtyMap);

		setProxy.add("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
