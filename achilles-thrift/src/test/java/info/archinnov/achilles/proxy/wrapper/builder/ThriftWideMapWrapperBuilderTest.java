package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.ThriftCompositeHelper;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
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
	private AchillesEntityInterceptor<Integer> interceptor;

	@Mock
	private ThriftCompositeHelper thriftCompositeHelper;

	@Mock
	private ThriftKeyValueFactory thriftKeyValueFactory;

	@Mock
	private ThriftIteratorFactory thriftIteratorFactory;

	@Mock
	private ThriftCompositeFactory thriftCompositeFactory;

	@Test
	public void should_build() throws Exception
	{
		ThriftWideMapWrapper<Integer, String> wrapper = ThriftWideMapWrapperBuilder
				.builder(1, dao, propertyMeta)
				.interceptor(interceptor)
				.thriftCompositeHelper(thriftCompositeHelper)
				.thriftKeyValueFactory(thriftKeyValueFactory)
				.thriftIteratorFactory(thriftIteratorFactory)
				.thriftCompositeFactory(thriftCompositeFactory)
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs((AchillesEntityInterceptor) interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "dao")).isSameAs(dao);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "interceptor")).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "thriftCompositeHelper")).isSameAs(
				thriftCompositeHelper);
		assertThat(Whitebox.getInternalState(wrapper, "thriftKeyValueFactory")).isSameAs(
				thriftKeyValueFactory);
		assertThat(Whitebox.getInternalState(wrapper, "thriftIteratorFactory")).isSameAs(
				thriftIteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "thriftCompositeFactory")).isSameAs(
				thriftCompositeFactory);
	}
}
