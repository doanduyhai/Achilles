package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.JoinExternalWideMapWrapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * JoinExternalWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class JoinExternalWideMapWrapperBuilderTest
{
	@Mock
	private GenericColumnFamilyDao<Integer, Long> dao;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private AchillesInterceptor interceptor;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityIntrospector entityIntrospector;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private CompositeKeyFactory compositeKeyFactory;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Test
	public void should_build() throws Exception
	{
		JoinExternalWideMapWrapper<Integer, Long, Integer, String> wrapper = JoinExternalWideMapWrapperBuilder
				.builder(1, dao, propertyMeta) //
				.interceptor(interceptor) //
				.compositeHelper(compositeHelper) //
				.compositeKeyFactory(compositeKeyFactory) //
				.entityHelper(entityIntrospector) //
				.iteratorFactory(iteratorFactory)//
				.keyValueFactory(keyValueFactory) //
				.loader(loader) //
				.persister(persister) //
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.getInterceptor()).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "dao")).isSameAs(dao);
		assertThat(Whitebox.getInternalState(wrapper, "interceptor")).isSameAs(interceptor);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "compositeHelper")).isSameAs(compositeHelper);
		assertThat(Whitebox.getInternalState(wrapper, "compositeKeyFactory")).isSameAs(
				compositeKeyFactory);
		assertThat(Whitebox.getInternalState(wrapper, "entityHelper")).isSameAs(entityIntrospector);
		assertThat(Whitebox.getInternalState(wrapper, "iteratorFactory")).isSameAs(iteratorFactory);
		assertThat(Whitebox.getInternalState(wrapper, "keyValueFactory")).isSameAs(keyValueFactory);
		assertThat(Whitebox.getInternalState(wrapper, "loader")).isSameAs(loader);
		assertThat(Whitebox.getInternalState(wrapper, "persister")).isSameAs(persister);
	}
}
