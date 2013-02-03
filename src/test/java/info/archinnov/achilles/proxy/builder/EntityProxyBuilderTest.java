package info.archinnov.achilles.proxy.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.builder.EntityProxyBuilder;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.lang.reflect.Method;
import java.util.Map;

import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Factory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * EntityProxyBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityProxyBuilderTest
{

	private EntityProxyBuilder builder = new EntityProxyBuilder();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Test
	public void should_build_proxy() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		CompleteBean proxy = builder.build(entity, entityMeta);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;

		assertThat(factory.getCallbacks()).hasSize(1);
		assertThat(factory.getCallback(0)).isInstanceOf(JpaEntityInterceptor.class);

	}

}
