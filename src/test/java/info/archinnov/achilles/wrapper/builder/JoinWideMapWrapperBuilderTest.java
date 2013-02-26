package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.wrapper.JoinWideMapWrapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * JoinWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class JoinWideMapWrapperBuilderTest
{
	@Mock
	private GenericDynamicCompositeDao<Integer> entityDao;

	@Mock
	private PropertyMeta<Integer, String> propertyMeta;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Test
	public void should_build() throws Exception
	{
		JoinWideMapWrapper<Integer, Integer, String> wrapper = JoinWideMapWrapperBuilder
				.builder(1, entityDao, propertyMeta) //
				.loader(loader) //
				.persister(persister) //
				.build();

		assertThat(wrapper).isNotNull();
		assertThat(Whitebox.getInternalState(wrapper, "entityDao")).isSameAs(entityDao);
		assertThat(Whitebox.getInternalState(wrapper, "propertyMeta")).isSameAs(propertyMeta);
		assertThat(Whitebox.getInternalState(wrapper, "loader")).isSameAs(loader);
		assertThat(Whitebox.getInternalState(wrapper, "persister")).isSameAs(persister);
	}
}
