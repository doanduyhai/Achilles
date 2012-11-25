package fr.doan.achilles.proxy.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Factory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;
import fr.doan.achilles.proxy.interceptor.JpaInterceptor;

@RunWith(MockitoJUnitRunner.class)
public class EntityProxyBuilderTest
{

	private EntityProxyBuilder<Long> builder = new EntityProxyBuilder<Long>();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private Map<Method, PropertyMeta<?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?>> setterMetas;

	@Mock
	private PropertyMeta<Long> idMeta;

	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

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
		when(entityMeta.getDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		CompleteBean proxy = builder.build(entity, entityMeta);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;

		assertThat(factory.getCallbacks()).hasSize(1);
		assertThat(factory.getCallback(0)).isInstanceOf(JpaInterceptor.class);

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_build_proxy_with_dirty_map() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		Set<Method> lazyLoaded = new HashSet<Method>();
		CompleteBean proxy = builder.build(entity, entityMeta, lazyLoaded);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;
		JpaInterceptor interceptor = (JpaInterceptor) factory.getCallback(0);

		assertThat(interceptor.getLazyLoaded()).isSameAs(lazyLoaded);

	}
}
