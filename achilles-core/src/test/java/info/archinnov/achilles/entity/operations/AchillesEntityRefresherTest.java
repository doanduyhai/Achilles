package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.AchillesEntityIntrospector;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;

/**
 * AchillesEntityRefresherTest
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
	private AchillesEntityLoader loader;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private AchillesEntityInterceptor<CompleteBean> jpaEntityInterceptor;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Mock
	private Set<Method> lazyLoaded;

	@Mock
	private AchillesPersistenceContext context;

	@Test
	public void should_refresh() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getPrimaryKey()).thenReturn(bean.getId());
		when(context.getEntity()).thenReturn(bean);

		when(proxifier.getInterceptor(bean)).thenReturn(jpaEntityInterceptor);

		when(jpaEntityInterceptor.getTarget()).thenReturn(bean);
		when(jpaEntityInterceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(jpaEntityInterceptor.getAlreadyLoaded()).thenReturn(lazyLoaded);
		when(loader.load(context, CompleteBean.class)).thenReturn(bean);

		achillesEntityRefresher.refresh(context);

		verify(dirtyMap).clear();
		verify(lazyLoaded).clear();
		verify(jpaEntityInterceptor).setTarget(bean);
	}
}
