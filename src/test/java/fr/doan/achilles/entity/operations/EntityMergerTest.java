package fr.doan.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Factory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.proxy.EntityProxyUtil;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;
import fr.doan.achilles.proxy.interceptor.JpaInterceptor;

@RunWith(MockitoJUnitRunner.class)
public class EntityMergerTest
{

	@InjectMocks
	private EntityMerger merger;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityProxyBuilder<Long> interceptorBuilder;

	@Mock
	private JpaInterceptor<Long> interceptor;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<String> propertyMeta;

	@Mock
	private Map<Method, PropertyMeta<?>> dirtyMap;

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private EntityProxyUtil util;

	@Mock
	private Bean entity;

	@Test
	public void should_persist_if_not_proxy() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

		when(interceptorBuilder.build(entity, entityMeta)).thenReturn(entity);

		CompleteBean mergedEntity = merger.mergeEntity(entity, entityMeta);

		assertThat(mergedEntity).isSameAs(entity);

		verify(persister).persist(entity, entityMeta);
	}

	@Test
	public void should_merge_proxy_with_simple_dirty() throws Exception
	{
		Factory factory = (Factory) entity;

		when(util.isProxy(entity)).thenReturn(true);

		when(factory.getCallback(0)).thenReturn(interceptor);
		when(entityMeta.getDao()).thenReturn(dao);

		Method ageSetter = CompleteBean.class.getDeclaredMethod("setAge", Long.class);
		Map<Method, PropertyMeta<?>> dirty = new HashMap<Method, PropertyMeta<?>>();
		dirty.put(ageSetter, propertyMeta);

		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(dirtyMap.entrySet()).thenReturn(dirty.entrySet());
		when(interceptor.getKey()).thenReturn(1L);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SIMPLE);

		CompleteBean mergedEntity = merger.mergeEntity(entity, entityMeta);

		assertThat(mergedEntity).isSameAs(entity);

		verify(persister).persistProperty(entity, 1L, dao, propertyMeta);
		verify(dirtyMap).clear();
	}

	@Test
	public void should_merge_proxy_with_multi_value_dirty() throws Exception
	{
		Factory factory = (Factory) entity;

		when(util.isProxy(entity)).thenReturn(true);

		when(factory.getCallback(0)).thenReturn(interceptor);
		when(entityMeta.getDao()).thenReturn(dao);

		Method ageSetter = CompleteBean.class.getDeclaredMethod("setAge", Long.class);
		Map<Method, PropertyMeta<?>> dirty = new HashMap<Method, PropertyMeta<?>>();
		dirty.put(ageSetter, propertyMeta);

		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(dirtyMap.entrySet()).thenReturn(dirty.entrySet());
		when(interceptor.getKey()).thenReturn(1L);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LAZY_SET);

		CompleteBean mergedEntity = merger.mergeEntity(entity, entityMeta);

		assertThat(mergedEntity).isSameAs(entity);

		verify(persister).removeProperty(1L, dao, propertyMeta);
		verify(persister).persistProperty(entity, 1L, dao, propertyMeta);
		verify(dirtyMap).clear();
	}

	@Test
	public void should_merge_proxy_with_no_dirty() throws Exception
	{
		Factory factory = (Factory) entity;

		when(util.isProxy(entity)).thenReturn(true);

		when(factory.getCallback(0)).thenReturn(interceptor);
		when(entityMeta.getDao()).thenReturn(dao);

		Map<Method, PropertyMeta<?>> dirty = new HashMap<Method, PropertyMeta<?>>();

		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(dirtyMap.entrySet()).thenReturn(dirty.entrySet());

		CompleteBean mergedEntity = merger.mergeEntity(entity, entityMeta);

		assertThat(mergedEntity).isSameAs(entity);

		verifyZeroInteractions(persister);
		verify(dirtyMap).clear();
	}

	class Bean extends CompleteBean implements Factory
	{
		public static final long serialVersionUID = 1L;

		@Override
		public Object newInstance(Callback callback)
		{
			return null;
		}

		@Override
		public Object newInstance(Callback[] callbacks)
		{
			return null;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Object newInstance(Class[] types, Object[] args, Callback[] callbacks)
		{
			return null;
		}

		@Override
		public Callback getCallback(int index)
		{
			return null;
		}

		@Override
		public void setCallback(int index, Callback callback)
		{}

		@Override
		public void setCallbacks(Callback[] callbacks)
		{}

		@Override
		public Callback[] getCallbacks()
		{
			return null;
		}

	}
}
