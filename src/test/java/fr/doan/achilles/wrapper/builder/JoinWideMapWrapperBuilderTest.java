package fr.doan.achilles.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.JoinWideMapMeta;
import fr.doan.achilles.wrapper.JoinWideMapWrapper;

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
	private GenericEntityDao<Integer> dao;

	@Mock
	private JoinWideMapMeta<Integer, String> joinWideMapMeta;

	@Test
	public void should_build() throws Exception
	{
		JoinWideMapWrapper<Integer, Integer, String> wrapper = JoinWideMapWrapperBuilder.builder(1,
				dao, joinWideMapMeta).build();

		assertThat(wrapper).isNotNull();
	}
}
