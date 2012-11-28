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
import fr.doan.achilles.wrapper.KeySetProxy;

@RunWith(MockitoJUnitRunner.class)
public class ValueCollectionProxyBuilderTest
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

		KeySetProxy<Integer> proxy = KeySetProxyBuilder.builder(targetMap.keySet()).dirtyMap(dirtyMap).setter(setter)
				.propertyMeta((PropertyMeta) propertyMeta).build();

		proxy.remove(1);

		verify(dirtyMap).put(setter, propertyMeta);

	}
}
