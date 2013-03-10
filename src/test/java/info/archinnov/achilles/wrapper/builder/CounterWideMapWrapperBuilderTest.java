package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.CounterWideMapWrapper;

import org.junit.Test;
import org.mockito.Mock;
import org.powermock.reflect.Whitebox;

/**
 * CounterWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterWideMapWrapperBuilderTest
{
	@Mock
	private CounterDao counterDao;

	@Mock
	private PropertyMeta<Integer, Long> propertyMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private AchillesInterceptor interceptor;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private CompositeKeyFactory compositeKeyFactory;

	@Mock
	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory;

	private String fqcn = "fqcn";

	@Test
	public void should_build() throws Exception
	{
		CounterWideMapWrapper<Long, Integer> wrapper = CounterWideMapWrapperBuilder
				.builder(1L, counterDao, propertyMeta) //
				.fqcn(fqcn) //
				.idMeta(idMeta) //
				.interceptor(interceptor) //
				.compositeHelper(compositeHelper) //
				.iteratorFactory(iteratorFactory) //
				.compositeKeyFactory(compositeKeyFactory) //
				.dynamicCompositeKeyFactory(dynamicCompositeKeyFactory) //
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "counterDao")).isSameAs(counterDao);
		assertThat(Whitebox.getInternalState(wrapper, "fqcn")).isSameAs(fqcn);
		assertThat(Whitebox.getInternalState(wrapper, "idMeta")).isSameAs(idMeta);
		assertThat(Whitebox.getInternalState(wrapper, "compositeHelper")).isSameAs(compositeHelper);
		assertThat(Whitebox.getInternalState(wrapper, "iteratorFactory")).isSameAs(iteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "keyValueFactory")).isSameAs(keyValueFactory);
		assertThat(Whitebox.getInternalState(wrapper, "compositeKeyFactory")).isSameAs(
				compositeKeyFactory);
		assertThat(Whitebox.getInternalState(wrapper, "dynamicCompositeKeyFactory")).isSameAs(
				dynamicCompositeKeyFactory);
	}
}
