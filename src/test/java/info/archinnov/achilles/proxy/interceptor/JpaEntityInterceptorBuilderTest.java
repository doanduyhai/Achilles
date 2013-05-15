package info.archinnov.achilles.proxy.interceptor;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapping.entity.WideRowBean;
import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * JpaEntityInterceptorBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class JpaEntityInterceptorBuilderTest
{
	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private ThriftGenericEntityDao<Long> dao;

	@Mock
	private ThriftGenericWideRowDao<Long, String> columnFamilyDao;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;

	@Mock
	private Set<Method> lazyLoaded;

	private ThriftPersistenceContext<Long> context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private ThriftGenericEntityDao<Long> entityDao;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder //
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId()) //
				.entity(entity) //
				.entityDao(entityDao) //
				.columnFamilyDao(columnFamilyDao) //
				.build();
	}

	@Test
	public void should_build_entity() throws Exception
	{
		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		Method idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		JpaEntityInterceptor<Long, CompleteBean> interceptor = JpaEntityInterceptorBuilder.builder(
				context, entity).build();

		assertThat(interceptor.getKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getTarget()).isEqualTo(entity);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getLazyAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getLazyAlreadyLoaded()).isInstanceOf(HashSet.class);

		assertThat(context.isWideRow()).isFalse();

		Object entityLoader = Whitebox.getInternalState(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(ThriftEntityLoader.class);
	}

	@Test
	public void should_build_wide_row() throws Exception
	{
		WideRowBean bean = new WideRowBean();
		bean.setId(1545L);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);

		Method idGetter = WideRowBean.class.getDeclaredMethod("getId");
		Method idSetter = WideRowBean.class.getDeclaredMethod("setId", Long.class);

		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(PropertyType.SIMPLE);

		idMeta.setGetter(idGetter);
		idMeta.setSetter(idSetter);

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.isWideRow()).thenReturn(true);

		JpaEntityInterceptor<Long, WideRowBean> interceptor = JpaEntityInterceptorBuilder
				.builder(context, bean).build();

		assertThat(interceptor.getKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getTarget()).isEqualTo(bean);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getLazyAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getLazyAlreadyLoaded()).isInstanceOf(HashSet.class);

		assertThat(context.isWideRow()).isTrue();

		Object entityLoader = Whitebox.getInternalState(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(ThriftEntityLoader.class);
	}
}
