package fr.doan.achilles.proxy.interceptor;

import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.WideRowBean;
import net.sf.cglib.proxy.MethodProxy;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.wrapper.ListWrapper;
import fr.doan.achilles.wrapper.MapWrapper;
import fr.doan.achilles.wrapper.SetWrapper;
import fr.doan.achilles.wrapper.WideRowWrapper;

@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
@RunWith(MockitoJUnitRunner.class)
public class JpaEntityInterceptorTest
{

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

	@Mock
	private EntityMeta<Long> entityMeta;

	private JpaEntityInterceptor<Long> interceptor;

	@Mock
	private GenericEntityDao<Long> dao;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

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

		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		interceptor = JpaInterceptorBuilder.builder(entityMeta).target(entity)
				.lazyLoaded(lazyLoaded).build();

		ReflectionTestUtils.setField(interceptor, "loader", loader);
		ReflectionTestUtils.setField(interceptor, "dirtyMap", dirtyMap);
		ReflectionTestUtils.setField(interceptor, "wideRow", false);
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
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SIMPLE);
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
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SIMPLE);

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

	@Test
	public void should_create_list_wrapper() throws Throwable
	{
		when(getterMetas.containsKey(nameGetter)).thenReturn(true);
		when(getterMetas.get(nameGetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(false);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LIST);

		when(proxy.invoke(entity, null)).thenReturn(Arrays.asList("a"));

		Object name = this.interceptor.intercept(entity, nameGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(ListWrapper.class);
	}

	@Test
	public void should_create_set_wrapper() throws Throwable
	{
		when(getterMetas.containsKey(nameGetter)).thenReturn(true);
		when(getterMetas.get(nameGetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(false);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SET);

		when(proxy.invoke(entity, null)).thenReturn(new HashSet<String>());

		Object name = this.interceptor.intercept(entity, nameGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(SetWrapper.class);
	}

	@Test
	public void should_create_map_wrapper() throws Throwable
	{
		when(getterMetas.containsKey(nameGetter)).thenReturn(true);
		when(getterMetas.get(nameGetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(false);
		when(propertyMeta.propertyType()).thenReturn(LAZY_MAP);

		when(proxy.invoke(entity, null)).thenReturn(new HashMap<Integer, String>());

		Object name = this.interceptor.intercept(entity, nameGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(MapWrapper.class);
	}

	@Test
	public void should_create_widerow_wrapper() throws Throwable
	{
		WideRowBean bean = new WideRowBean();
		Method mapGetter = WideRowBean.class.getDeclaredMethod("getMap");
		
		when(getterMetas.containsKey(mapGetter)).thenReturn(true);
		when(getterMetas.get(mapGetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(false);
		when(propertyMeta.propertyType()).thenReturn(WIDE_MAP);
		ReflectionTestUtils.setField(interceptor, "wideRow", true);

		Object name = this.interceptor.intercept(bean, mapGetter, (Object[]) null, proxy);

		assertThat(name).isInstanceOf(WideRowWrapper.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_call_setter_on_wide_map() throws Throwable
	{
		when(setterMetas.containsKey(nameSetter)).thenReturn(true);
		when(setterMetas.get(nameSetter)).thenReturn(propertyMeta);
		when(propertyMeta.isLazy()).thenReturn(true);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.WIDE_MAP);

		this.interceptor.intercept(entity, nameSetter, (Object[]) null, proxy);
	}
}
