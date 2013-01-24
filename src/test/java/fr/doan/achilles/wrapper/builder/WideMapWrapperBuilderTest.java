package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.WideMapWrapper;

/**
 * WideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class WideMapWrapperBuilderTest
{
	@Mock
	private GenericDynamicCompositeDao<Integer> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Test
	public void should_build() throws Exception
	{
		WideMapWrapper<Integer, Integer, String> wrapper = WideMapWrapperBuilder.builder(1, dao,
				wideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
