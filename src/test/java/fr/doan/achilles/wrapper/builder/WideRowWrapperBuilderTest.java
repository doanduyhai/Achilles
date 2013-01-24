package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.ExternalWideMapWrapper;

/**
 * WideRowWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class WideRowWrapperBuilderTest
{
	@Mock
	private GenericCompositeDao<Integer, String> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Test
	public void should_build() throws Exception
	{
		ExternalWideMapWrapper<Integer, Integer, String> wrapper = WideRowWrapperBuilder.builder(1, dao,
				wideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
