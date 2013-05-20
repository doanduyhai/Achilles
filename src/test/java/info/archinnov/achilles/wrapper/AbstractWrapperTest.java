package info.archinnov.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * AbstractWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AbstractWrapperTest
{
	@Mock
	private AbstractWrapper<Void, String> wrapper;

	private Map<Method, PropertyMeta<?, ?>> dirtyMap = new HashMap<Method, PropertyMeta<?, ?>>();

	private PropertyMeta<Void, String> propertyMeta;

	@Before
	public void setUp() throws Exception
	{
		dirtyMap.clear();
		doCallRealMethod().when(wrapper).setDirtyMap(dirtyMap);
		wrapper.setDirtyMap(dirtyMap);

		propertyMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		doCallRealMethod().when(wrapper).setPropertyMeta(propertyMeta);
		wrapper.setPropertyMeta(propertyMeta);

		doCallRealMethod().when(wrapper).setSetter(propertyMeta.getSetter());
		wrapper.setSetter(propertyMeta.getSetter());
	}

	@Test
	public void should_mark_dirty() throws Exception
	{
		doCallRealMethod().when(wrapper).markDirty();
		wrapper.markDirty();

		assertThat(dirtyMap).containsKey(propertyMeta.getSetter());
		assertThat(dirtyMap).containsValue(propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_if_already_dirty() throws Exception
	{
		dirtyMap.put(propertyMeta.getSetter(), propertyMeta);
		doCallRealMethod().when(wrapper).markDirty();

		wrapper.markDirty();

		assertThat(dirtyMap).hasSize(1);
		assertThat(dirtyMap).containsValue(propertyMeta);
	}
}
