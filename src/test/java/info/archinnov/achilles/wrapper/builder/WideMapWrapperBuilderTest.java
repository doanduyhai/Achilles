package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.WideMapWrapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * WideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class WideMapWrapperBuilderTest
{
	@Mock
	private GenericDynamicCompositeDao<Integer> entityDao;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private AchillesInterceptor interceptor;

	@Mock
	private EntityIntrospector entityIntrospector;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Test
	public void should_build() throws Exception
	{
		WideMapWrapper<Integer, Integer, String> wrapper = WideMapWrapperBuilder
				.builder(1, entityDao, propertyMeta) //
				.interceptor(interceptor) //
				.compositeHelper(compositeHelper) //
				.entityHelper(entityIntrospector) //
				.iteratorFactory(iteratorFactory) //
				.keyFactory(keyFactory) //
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "entityDao")).isSameAs(entityDao);
		assertThat(Whitebox.getInternalState(wrapper, "compositeHelper")).isSameAs(compositeHelper);
		assertThat(Whitebox.getInternalState(wrapper, "entityHelper")).isSameAs(entityIntrospector);
		assertThat(Whitebox.getInternalState(wrapper, "iteratorFactory")).isSameAs(iteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "keyFactory")).isSameAs(keyFactory);
	}
}
