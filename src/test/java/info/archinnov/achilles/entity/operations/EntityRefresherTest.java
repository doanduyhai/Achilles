package info.archinnov.achilles.entity.operations;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

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
	private EntityIntrospector introspector;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private EntityValidator entityValidator;

	@Mock
	private EntityLoader loader;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private CompleteBean proxy;

	@Mock
	private JpaEntityInterceptor<Object, CompleteBean> jpaEntityInterceptor;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Mock
	private Set<Method> lazyLoaded;

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(entityRefresher, "introspector", introspector);
		Whitebox.setInternalState(entityRefresher, "proxifier", proxifier);
		Whitebox.setInternalState(entityRefresher, "entityValidator", entityValidator);
		Whitebox.setInternalState(entityRefresher, "loader", loader);
	}

	@Test
	public void should_refresh() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(proxifier.isProxy(proxy)).thenReturn(true);
		when(proxifier.getInterceptor(proxy)).thenReturn(jpaEntityInterceptor);

		when(jpaEntityInterceptor.getTarget()).thenReturn(bean);
		when(jpaEntityInterceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(jpaEntityInterceptor.getLazyAlreadyLoaded()).thenReturn(lazyLoaded);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(introspector.determinePrimaryKey(proxy, entityMeta)).thenReturn(12L);
		when(loader.load(eq(CompleteBean.class), eq(12L), eq(entityMeta))).thenReturn(bean);

		entityRefresher.refresh(proxy, entityMetaMap);

		verify(entityValidator).validateEntity(proxy, entityMetaMap);
		verify(dirtyMap).clear();
		verify(lazyLoaded).clear();
		verify(jpaEntityInterceptor).setTarget(bean);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_entity_is_not_managed() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		entityRefresher.refresh(bean, entityMetaMap);
	}
}
