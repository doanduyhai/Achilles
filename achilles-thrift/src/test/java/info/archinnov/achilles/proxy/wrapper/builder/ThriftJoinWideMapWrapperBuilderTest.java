package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.helper.ThriftCompositeHelper;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
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
	private AchillesEntityInterceptor<Integer> interceptor;

	@Mock
	private ThriftEntityPersister persister;

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private AchillesEntityProxifier proxifier;

	@Mock
	private ThriftCompositeHelper thriftCompositeHelper;

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
				.thriftCompositeHelper(thriftCompositeHelper)
				.thriftCompositeFactory(thriftCompositeFactory)
				.thriftIteratorFactory(thriftIteratorFactory)
				.thriftKeyValueFactory(thriftKeyValueFactory)
				.loader(loader)
				.persister(persister)
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs((AchillesEntityInterceptor) interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "dao")).isSameAs(dao);
		assertThat(Whitebox.getInternalState(wrapper, "interceptor")).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "thriftCompositeHelper")).isSameAs(
				thriftCompositeHelper);
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
