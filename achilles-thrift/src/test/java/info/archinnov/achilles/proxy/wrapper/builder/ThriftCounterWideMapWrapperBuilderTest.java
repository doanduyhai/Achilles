package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
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
	private ThriftEntityInterceptor<Long> interceptor;

	@Mock
	private ThriftPersistenceContext context;

	@Test
	public void should_build() throws Exception
	{
		ThriftCounterWideMapWrapper<Integer> wrapper = ThriftCounterWideMapWrapperBuilder
				.builder(1L, wideMapCounterDao, propertyMeta)
				.interceptor(interceptor)
				.context(context)
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs((ThriftEntityInterceptor) interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "wideMapCounterDao")).isSameAs(
				wideMapCounterDao);
		assertThat(Whitebox.getInternalState(wrapper, "context")).isSameAs(context);
	}
}
