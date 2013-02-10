package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.ExternalWideMapWrapper;
import info.archinnov.achilles.wrapper.builder.ExternalWideMapWrapperBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * WideRowWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ExternalWideMapWrapperBuilderTest
{
	@Mock
	private GenericCompositeDao<Integer, String> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Test
	public void should_build() throws Exception
	{
		ExternalWideMapWrapper<Integer, Integer, String> wrapper = ExternalWideMapWrapperBuilder.builder(1, dao,
				wideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
