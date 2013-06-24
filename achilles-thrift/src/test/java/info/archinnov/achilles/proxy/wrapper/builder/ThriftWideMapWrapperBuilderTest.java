package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.ThriftWideMapWrapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftWideMapWrapperBuilderTest
{
	@Mock
	private ThriftGenericWideRowDao dao;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private ThriftEntityInterceptor<Integer> interceptor;

	@Test
	public void should_build() throws Exception
	{
		ThriftWideMapWrapper<Integer, String> wrapper = ThriftWideMapWrapperBuilder
				.builder(1, dao, propertyMeta)
				.interceptor(interceptor)
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs((ThriftEntityInterceptor) interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "dao")).isSameAs(dao);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "interceptor")).isSameAs(interceptor);
	}
}
