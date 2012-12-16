package fr.doan.achilles.entity.operations;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Factory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.proxy.interceptor.JpaInterceptor;

/**
 * EntityRefresherTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
public class EntityRefresherTest
{

	@InjectMocks
	private EntityRefresher entityRefresher;

	@Mock
	private EntityWrapperUtil util;

	@Mock
	private EntityValidator entityValidator;

	@Mock
	private EntityLoader loader;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private Factory proxy;

	@Mock
	private JpaInterceptor<Long> jpaInterceptor;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Mock
	private Set<Method> lazyLoaded;

	@Test
	public void should_refresh() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(proxy.getCallback(0)).thenReturn(jpaInterceptor);
		when(jpaInterceptor.getTarget()).thenReturn(bean);
		when(jpaInterceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(jpaInterceptor.getLazyLoaded()).thenReturn(lazyLoaded);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(util.determinePrimaryKey(proxy, entityMeta)).thenReturn(12L);
		when(loader.load(eq(CompleteBean.class), eq(12L), eq(entityMeta))).thenReturn(bean);

		entityRefresher.refresh(proxy, entityMetaMap);

		verify(entityValidator).validateEntity(proxy, entityMetaMap);
		verify(dirtyMap).clear();
		verify(lazyLoaded).clear();
		verify(jpaInterceptor).setTarget(bean);
	}
}
