package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.JoinExternalWideMapWrapper;
import info.archinnov.achilles.wrapper.builder.JoinExternalWideMapWrapperBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


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
	private GenericCompositeDao<Integer, Long> dao;

	@Mock
	private PropertyMeta<Integer, String> joinExternalWideMapMeta;

	@Test
	public void should_build() throws Exception
	{
		JoinExternalWideMapWrapper<Integer, Long, Integer, String> wrapper = JoinExternalWideMapWrapperBuilder
				.builder(1, dao, joinExternalWideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
