package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.IteratorWrapper;
import info.archinnov.achilles.wrapper.builder.IteratorWrapperBuilder;

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
 * IteratorWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class IteratorWrapperBuilderTest
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

		IteratorWrapper<String> iteratorWrapper = IteratorWrapperBuilder.builder(target.iterator())
				.dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta).build();

		assertThat(iteratorWrapper.getDirtyMap()).isSameAs(dirtyMap);

		iteratorWrapper.next();
		iteratorWrapper.remove();

		verify(dirtyMap).put(setter, propertyMeta);
	}
}
