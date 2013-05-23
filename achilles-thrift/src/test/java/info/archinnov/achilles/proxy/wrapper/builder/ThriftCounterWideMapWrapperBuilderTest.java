package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.ThriftCompositeHelper;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWideMapWrapper;
import info.archinnov.achilles.type.Counter;

import org.junit.Test;
import org.mockito.Mock;
import org.powermock.reflect.Whitebox;

/**
 * ThriftCounterWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterWideMapWrapperBuilderTest
{
	@Mock
	private ThriftGenericWideRowDao wideMapCounterDao;

	@Mock
	private PropertyMeta<Integer, Counter> propertyMeta;

	@Mock
	private AchillesEntityInterceptor<Long> interceptor;

	@Mock
	private ThriftCompositeHelper thriftCompositeHelper;

	@Mock
	private ThriftKeyValueFactory thriftKeyValueFactory;

	@Mock
	private ThriftIteratorFactory thriftIteratorFactory;

	@Mock
	private ThriftCompositeFactory thriftCompositeFactory;

	@Mock
	private ThriftPersistenceContext context;

	@Test
	public void should_build() throws Exception
	{
		ThriftCounterWideMapWrapper<Integer> wrapper = ThriftCounterWideMapWrapperBuilder
				.builder(1L, wideMapCounterDao, propertyMeta)
				.interceptor(interceptor)
				.thriftCompositeHelper(thriftCompositeHelper)
				.thriftIteratorFactory(thriftIteratorFactory)
				.thriftCompositeFactory(thriftCompositeFactory)
				.context(context)
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs((AchillesEntityInterceptor) interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "wideMapCounterDao")).isSameAs(
				wideMapCounterDao);
		assertThat(Whitebox.getInternalState(wrapper, "context")).isSameAs(context);
		assertThat(Whitebox.getInternalState(wrapper, "thriftCompositeHelper")).isSameAs(
				thriftCompositeHelper);
		assertThat(Whitebox.getInternalState(wrapper, "thriftIteratorFactory")).isSameAs(
				thriftIteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "thriftKeyValueFactory")).isSameAs(
				thriftKeyValueFactory);
		assertThat(Whitebox.getInternalState(wrapper, "thriftCompositeFactory")).isSameAs(
				thriftCompositeFactory);
	}
}
