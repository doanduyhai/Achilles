package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;

import java.lang.reflect.Method;
import java.util.HashMap;

import mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftEntityProxifierTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityProxifierTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();

	@Mock
	private ThriftPersistenceContext context;

	@Test
	public void should_build_interceptor() throws Exception
	{
		ThriftGenericEntityDao entityDao = mock(ThriftGenericEntityDao.class);
		CompleteBean entity = new CompleteBean();
		Object primaryKey = 11L;

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.accessors()
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setGetterMetas(new HashMap<Method, PropertyMeta<?, ?>>());
		meta.setSetterMetas(new HashMap<Method, PropertyMeta<?, ?>>());
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		when(context.getEntityMeta()).thenReturn(meta);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityDao()).thenReturn(entityDao);
		when(context.getPrimaryKey()).thenReturn(primaryKey);

		AchillesEntityInterceptor<CompleteBean> interceptor = proxifier.buildInterceptor(context,
				entity);

		assertThat(interceptor).isInstanceOf(ThriftEntityInterceptor.class);
		assertThat(interceptor.getTarget()).isSameAs(entity);
		assertThat(interceptor.getContext()).isSameAs(context);

		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isEmpty();

		assertThat(interceptor.getKey()).isSameAs(primaryKey);
		assertThat(interceptor.getAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getAlreadyLoaded()).isEmpty();
	}

	@Test
	public void should_exception_when_no_wide_row_dao_found() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		EntityMeta meta = new EntityMeta();
		meta.setGetterMetas(new HashMap<Method, PropertyMeta<?, ?>>());
		meta.setSetterMetas(new HashMap<Method, PropertyMeta<?, ?>>());
		meta.setClassName("CompleteBean");
		meta.setWideRow(true);

		when(context.getEntityMeta()).thenReturn(meta);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);

		exception.expect(AchillesException.class);

		proxifier.buildInterceptor(context, entity);
	}
}
