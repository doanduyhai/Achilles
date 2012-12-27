package fr.doan.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import mapping.entity.UserBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.JoinMetaHolder;
import fr.doan.achilles.entity.metadata.JoinWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;

/**
 * JoinEntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class JoinEntityLoaderTest
{

	private JoinEntityLoader joinEntityLoader = new JoinEntityLoader();

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityProxyBuilder interceptorBuilder;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(joinEntityLoader, "loader", loader);
		ReflectionTestUtils.setField(joinEntityLoader, "interceptorBuilder", interceptorBuilder);
	}

	@Test
	public void should_load_join_entity() throws Exception
	{
		Long joinId = 45L;

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		JoinMetaHolder<Long> joinMetaData = new JoinMetaHolder<Long>();
		joinMetaData.setEntityMeta(entityMeta);

		PropertyMeta<Integer, UserBean> joinPropertyMeta = new JoinWideMapMeta<Integer, UserBean>();
		joinPropertyMeta.setValueClass(UserBean.class);
		joinPropertyMeta.setJoinMetaHolder(joinMetaData);

		UserBean userBean = new UserBean();

		when(loader.load(UserBean.class, joinId, entityMeta)).thenReturn(userBean);
		when(interceptorBuilder.build(userBean, entityMeta)).thenReturn(userBean);

		UserBean expected = this.joinEntityLoader.loadJoinEntity(joinId, joinPropertyMeta);

		assertThat(expected).isSameAs(userBean);

		verify(interceptorBuilder).build(userBean, entityMeta);

	}
}
