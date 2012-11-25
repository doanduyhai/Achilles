package fr.doan.achilles.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
import fr.doan.achilles.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
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
	private GenericDao<Long> dao;

	@Mock
	private EntityProxyUtil util;

	@Test
	public void should_persist() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

		when(interceptorBuilder.build(entity, entityMeta)).thenReturn(entity);

		CompleteBean mergedEntity = merger.mergeEntity(entity, entityMeta);

		assertThat(mergedEntity).isSameAs(entity);

		verify(persister).persist(entity, entityMeta);
	}

	@Test
	public void should_merge() throws Exception
	{
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
		Bean entity = mock(Bean.class);
		Factory factory = (Factory) entity;

		when(util.isProxy(entity)).thenReturn(true);

		when(factory.getCallback(0)).thenReturn(interceptor);
		when(entityMeta.getDao()).thenReturn(dao);

		Method ageSetter = CompleteBean.class.getDeclaredMethod("setAge", Long.class);
		Map<Method, PropertyMeta<?>> dirtyMap = new HashMap<Method, PropertyMeta<?>>();
		dirtyMap.put(ageSetter, propertyMeta);

		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(interceptor.getKey()).thenReturn(1L);
		when(interceptor.getTarget()).thenReturn(entity);
		when(interceptor.getLazyLoaded()).thenReturn(null);
		when(interceptorBuilder.build(entity, entityMeta, null)).thenReturn(entity);

		CompleteBean mergedEntity = merger.mergeEntity(entity, entityMeta);

		assertThat(mergedEntity).isSameAs(entity);

		verify(persister).persistProperty(entity, 1L, dao, propertyMeta);
	}
}
