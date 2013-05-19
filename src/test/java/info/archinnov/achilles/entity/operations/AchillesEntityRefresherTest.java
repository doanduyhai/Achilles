package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * EntityRefresherTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class AchillesEntityRefresherTest
{

	@InjectMocks
	private AchillesEntityRefresher achillesEntityRefresher;

	@Mock
	private AchillesEntityIntrospector introspector;

	@Mock
	private AchillesEntityProxifier proxifier;

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private JpaEntityInterceptor<CompleteBean> jpaEntityInterceptor;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Mock
	private Set<Method> lazyLoaded;

	@Test
	public void should_refresh() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		ThriftPersistenceContext context = PersistenceContextTestBuilder //
				.mockAll(entityMeta, CompleteBean.class, bean.getId())
				.entity(bean)
				.build();
		when(proxifier.getInterceptor(bean)).thenReturn(jpaEntityInterceptor);

		when(jpaEntityInterceptor.getTarget()).thenReturn(bean);
		when(jpaEntityInterceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(jpaEntityInterceptor.getLazyAlreadyLoaded()).thenReturn(lazyLoaded);
		when(loader.load(context, CompleteBean.class)).thenReturn(bean);

		achillesEntityRefresher.refresh(context);

		verify(dirtyMap).clear();
		verify(lazyLoaded).clear();
		verify(jpaEntityInterceptor).setTarget(bean);
	}
}
