package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.WideMapWrapper;
import info.archinnov.achilles.wrapper.builder.WideMapWrapperBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


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
