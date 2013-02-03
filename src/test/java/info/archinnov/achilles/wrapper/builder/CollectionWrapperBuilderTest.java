package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.CollectionWrapper;
import info.archinnov.achilles.wrapper.builder.CollectionWrapperBuilder;

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
 * CollectionWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectionWrapperBuilderTest
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
		CollectionWrapper<String> collectionWrapper = CollectionWrapperBuilder.builder(target)
				.dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();

		assertThat(collectionWrapper.getDirtyMap()).isSameAs(dirtyMap);

		collectionWrapper.add("a");

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
