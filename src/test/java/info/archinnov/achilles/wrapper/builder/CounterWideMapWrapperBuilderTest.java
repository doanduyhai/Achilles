package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.Counter;
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
	private GenericWideRowDao<Long, Long> wideMapCounterDao;

	@Mock
	private PropertyMeta<Integer, Counter> propertyMeta;

	@Mock
	private AchillesInterceptor<Long> interceptor;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private CompositeFactory compositeFactory;

	@Mock
	private PersistenceContext<Long> context;

	@Test
	public void should_build() throws Exception
	{
		CounterWideMapWrapper<Long, Integer> wrapper = CounterWideMapWrapperBuilder
				.builder(1L, wideMapCounterDao, propertyMeta) //
				.interceptor(interceptor) //
				.compositeHelper(compositeHelper) //
				.iteratorFactory(iteratorFactory) //
				.compositeFactory(compositeFactory) //
				.context(context) //
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "wideMapCounterDao")).isSameAs(
				wideMapCounterDao);
		assertThat(Whitebox.getInternalState(wrapper, "context")).isSameAs(context);
		assertThat(Whitebox.getInternalState(wrapper, "compositeHelper")).isSameAs(compositeHelper);
		assertThat(Whitebox.getInternalState(wrapper, "iteratorFactory")).isSameAs(iteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "keyValueFactory")).isSameAs(keyValueFactory);
		assertThat(Whitebox.getInternalState(wrapper, "compositeFactory")).isSameAs(
				compositeFactory);
	}
}
