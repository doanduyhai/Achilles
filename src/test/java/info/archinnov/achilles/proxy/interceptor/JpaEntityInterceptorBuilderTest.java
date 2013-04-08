package info.archinnov.achilles.proxy.interceptor;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapping.entity.ColumnFamilyBean;
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
	private GenericEntityDao<Long> dao;

	@Mock
	private GenericColumnFamilyDao<Long, String> columnFamilyDao;

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

	private PersistenceContext<Long> context;

	@Mock
	private CounterDao counterDao;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Mock
	private GenericEntityDao<Long> entityDao;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder //
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId()) //
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

		assertThat(context.isDirectColumnFamilyMapping()).isFalse();

		Object entityLoader = Whitebox.getInternalState(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(EntityLoader.class);
	}

	@Test
	public void should_build_column_family() throws Exception
	{
		ColumnFamilyBean bean = new ColumnFamilyBean();
		bean.setId(1545L);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);

		Method idGetter = ColumnFamilyBean.class.getDeclaredMethod("getId");
		Method idSetter = ColumnFamilyBean.class.getDeclaredMethod("setId", Long.class);

		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(PropertyType.SIMPLE);

		idMeta.setGetter(idGetter);
		idMeta.setSetter(idSetter);

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);

		JpaEntityInterceptor<Long, ColumnFamilyBean> interceptor = JpaEntityInterceptorBuilder
				.builder(context, bean).build();

		assertThat(interceptor.getKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getTarget()).isEqualTo(bean);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getLazyAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getLazyAlreadyLoaded()).isInstanceOf(HashSet.class);

		assertThat(context.isDirectColumnFamilyMapping()).isTrue();

		Object entityLoader = Whitebox.getInternalState(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(EntityLoader.class);
	}
}
