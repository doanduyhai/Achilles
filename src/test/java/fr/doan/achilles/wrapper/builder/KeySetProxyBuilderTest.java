package fr.doan.achilles.wrapper.builder;

import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.ValueCollectionProxy;

@RunWith(MockitoJUnitRunner.class)
public class KeySetProxyBuilderTest
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

		ValueCollectionProxy<String> proxy = ValueCollectionProxyBuilder.builder(targetMap.values()).dirtyMap(dirtyMap).setter(setter)
				.propertyMeta((PropertyMeta) propertyMeta).build();

		proxy.remove("FR");

		verify(dirtyMap).put(setter, propertyMeta);

	}
}
