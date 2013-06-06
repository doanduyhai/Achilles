package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.helper.ThriftPropertyHelper;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.ThriftJoinWideMapWrapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftJoinWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftJoinWideMapWrapperBuilderTest
{
	@Mock
	private ThriftGenericWideRowDao dao;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private ThriftEntityInterceptor<Integer> interceptor;

	@Mock
	private ThriftEntityPersister persister;

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private ThriftEntityProxifier proxifier;

	@Mock
	private ThriftPropertyHelper thriftPropertyHelper;

	@Mock
	private ThriftCompositeFactory thriftCompositeFactory;

	@Mock
	private ThriftKeyValueFactory thriftKeyValueFactory;

	@Mock
	private ThriftIteratorFactory thriftIteratorFactory;

	@Test
	public void should_build() throws Exception
	{
		ThriftJoinWideMapWrapper<Integer, String> wrapper = ThriftJoinWideMapWrapperBuilder
				.builder(1, dao, propertyMeta)
				.interceptor(interceptor)
				.proxifier(proxifier)
				.thriftPropertyHelper(thriftPropertyHelper)
				.thriftCompositeFactory(thriftCompositeFactory)
				.thriftIteratorFactory(thriftIteratorFactory)
				.thriftKeyValueFactory(thriftKeyValueFactory)
				.loader(loader)
				.persister(persister)
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs((ThriftEntityInterceptor) interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "dao")).isSameAs(dao);
		assertThat(Whitebox.getInternalState(wrapper, "interceptor")).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "thriftPropertyHelper")).isSameAs(
				thriftPropertyHelper);
		assertThat(Whitebox.getInternalState(wrapper, "thriftCompositeFactory")).isSameAs(
				thriftCompositeFactory);
		assertThat(Whitebox.getInternalState(wrapper, "proxifier")).isSameAs(proxifier);
		assertThat(Whitebox.getInternalState(wrapper, "thriftIteratorFactory")).isSameAs(
				thriftIteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "thriftKeyValueFactory")).isSameAs(
				thriftKeyValueFactory);
		assertThat(Whitebox.getInternalState(wrapper, "loader")).isSameAs(loader);
		assertThat(Whitebox.getInternalState(wrapper, "persister")).isSameAs(persister);
	}
}
