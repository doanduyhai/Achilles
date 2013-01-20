package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.JoinExternalWideRowWrapper;

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
		JoinExternalWideRowWrapper<Integer, Long, Integer, String> wrapper = JoinExternalWideMapWrapperBuilder
				.builder(1, dao, joinExternalWideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
