package info.archinnov.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.JoinWideMapWrapper;
import info.archinnov.achilles.wrapper.builder.JoinWideMapWrapperBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


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
	private GenericDynamicCompositeDao<Integer> dao;

	@Mock
	private PropertyMeta<Integer, String> joinWideMapMeta;

	@Test
	public void should_build() throws Exception
	{
		JoinWideMapWrapper<Integer, Integer, String> wrapper = JoinWideMapWrapperBuilder.builder(1,
				dao, joinWideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
