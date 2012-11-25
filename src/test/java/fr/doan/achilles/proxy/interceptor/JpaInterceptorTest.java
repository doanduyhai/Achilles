package fr.doan.achilles.proxy.interceptor;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.MethodProxy;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.operations.EntityLoader;

@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
@RunWith(MockitoJUnitRunner.class)
public class JpaInterceptorTest
{

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

	@Mock
	private EntityMeta<Long> entityMeta;

	private JpaInterceptor<Long> interceptor;

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private Map<Method, PropertyMeta<?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?>> setterMetas;

	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

	@Mock
	private Set<Method> lazyLoaded;

	@Mock
	private EntityLoader loader;

	@Mock
	private MethodProxy proxy;

	@Mock
	private PropertyMeta propertyMeta;

	private Method idGetter;

	private Method idSetter;

	private Method nameGetter;

	private Method nameSetter;

	@Before
	public void setUp() throws Exception
	{
		idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		nameGetter = CompleteBean.class.getDeclaredMethod("getName", (Class<?>[]) null);
		nameSetter = CompleteBean.class.getDeclaredMethod("setName", String.class);

		PropertyMeta<Long> idMeta = mock(PropertyMeta.class);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		interceptor = JpaInterceptorBuilder.builder(entityMeta).target(entity).lazyLoaded(lazyLoaded).build();

		ReflectionTestUtils.setField(interceptor, "loader", loader);
		ReflectionTestUtils.setField(interceptor, "dirtyMap", dirtyMap);
	}

	@Test
	public void should_get_id_value_directly() throws Throwable
	{
		Object key = this.interceptor.intercept(entity, idGetter, (Object[]) null, proxy);
		assertThat(key).isEqualTo(1L);
	}

	@Test(expected = IllegalAccessException.class)
	public void should_exception_when_setter_called_on_id() throws Throwable
	{
		this.interceptor.intercept(entity, idSetter, new Object[]
		{
			1L
		}, proxy);
	}

	@Test
	public void should_get_unmapped_property() throws Throwable
	{
		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");
		Object name = this.interceptor.intercept(entity, nameGetter, (Object[]) null, proxy);

		assertThat(name).isEqualTo("name");

		verify(getterMetas).containsKey(nameGetter);
		verify(setterMetas).containsKey(nameGetter);
	}

	@Test
	public void should_load_lazy_property() throws Throwable
	{
		when(getterMetas.containsKey(nameGetter)).thenReturn(true);
		when(getterMetas.get(nameGetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(true);
		when(lazyLoaded.contains(nameGetter)).thenReturn(false);

		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");

		Object name = this.interceptor.intercept(entity, nameGetter, (Object[]) null, proxy);

		assertThat(name).isEqualTo("name");

		verify(loader).loadPropertyIntoObject(entity, 1L, dao, propertyMeta);
		verify(lazyLoaded).add(nameGetter);
	}

	@Test
	public void should_return_already_loaded_lazy_property() throws Throwable
	{
		when(getterMetas.containsKey(nameGetter)).thenReturn(true);
		when(getterMetas.get(nameGetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(true);
		when(lazyLoaded.contains(nameGetter)).thenReturn(true);

		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");

		Object name = this.interceptor.intercept(entity, nameGetter, (Object[]) null, proxy);

		assertThat(name).isEqualTo("name");

		verifyZeroInteractions(loader);
		verify(lazyLoaded, never()).add(nameGetter);
	}

	@Test
	public void should_set_property() throws Throwable
	{
		when(setterMetas.containsKey(nameSetter)).thenReturn(true);
		when(setterMetas.get(nameSetter)).thenReturn(propertyMeta);

		Object[] args = new Object[]
		{
			"sdfsdvdqfv"
		};

		when(proxy.invoke(entity, args)).thenReturn(null);
		Object name = this.interceptor.intercept(entity, nameSetter, args, proxy);

		assertThat(name).isNull();

		verify(proxy).invoke(entity, args);
		verify(dirtyMap).put(nameSetter, propertyMeta);
	}
}
