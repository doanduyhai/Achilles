package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.entity.metadata.PropertyType.EMBEDDED_ID;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftEntityInterceptorBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityInterceptorBuilderTest
{
	@Mock
	private EntityMeta entityMeta;

	@Mock
	private ThriftGenericEntityDao dao;

	@Mock
	private ThriftGenericWideRowDao columnFamilyDao;

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

	private ThriftPersistenceContext context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private ThriftGenericEntityDao entityDao;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp()
	{
		context = ThriftPersistenceContextTestBuilder //
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity)
				.entityDao(entityDao)
				.wideRowDao(columnFamilyDao)
				.build();
	}

	@Test
	public void should_build_entity() throws Exception
	{
		List<Method> eagerMethods = new ArrayList<Method>();
		Method nameGetter = CompleteBean.class.getDeclaredMethod("getName");
		Method ageGetter = CompleteBean.class.getDeclaredMethod("getAge");
		eagerMethods.add(nameGetter);
		eagerMethods.add(ageGetter);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getEagerGetters()).thenReturn(eagerMethods);

		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);

		Method idGetter = CompleteBean.class.getDeclaredMethod("getId");
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		ThriftEntityInterceptor<CompleteBean> interceptor = ThriftEntityInterceptorBuilder.builder(
				context, entity).build();

		assertThat(interceptor.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getTarget()).isEqualTo(entity);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getAlreadyLoaded()).isInstanceOf(HashSet.class);
		assertThat(interceptor.getAlreadyLoaded()).containsOnly(nameGetter, ageGetter);

		assertThat(context.isClusteredEntity()).isFalse();

		Object entityLoader = Whitebox.getInternalState(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(ThriftEntityLoader.class);
	}

	@Test
	public void should_not_load_eager_fields() throws Exception
	{
		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);

		Method idGetter = CompleteBean.class.getDeclaredMethod("getId");
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);
		context.setLoadEagerFields(false);

		ThriftEntityInterceptor<CompleteBean> interceptor = ThriftEntityInterceptorBuilder.builder(
				context, entity).build();

		assertThat(interceptor.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getTarget()).isEqualTo(entity);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getAlreadyLoaded()).isEmpty();
	}

	@Test
	public void should_build_wide_row() throws Exception
	{
		CompoundKey embeddedId = new CompoundKey();
		BeanWithClusteredId bean = new BeanWithClusteredId();
		bean.setId(embeddedId);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);

		Method idGetter = BeanWithClusteredId.class.getDeclaredMethod("getId");
		Method idSetter = BeanWithClusteredId.class.getDeclaredMethod("setId", CompoundKey.class);

		PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.type(EMBEDDED_ID)
				.build();

		idMeta.setGetter(idGetter);
		idMeta.setSetter(idSetter);

		when((PropertyMeta<Void, CompoundKey>) entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.isClusteredEntity()).thenReturn(true);

		context = ThriftPersistenceContextTestBuilder //
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, embeddedId)
				.entity(bean)
				.entityDao(entityDao)
				.wideRowDao(columnFamilyDao)
				.build();

		ThriftEntityInterceptor<BeanWithClusteredId> interceptor = ThriftEntityInterceptorBuilder
				.builder(
						context, bean)
				.build();

		assertThat(interceptor.getPrimaryKey()).isEqualTo(embeddedId);
		assertThat(interceptor.getTarget()).isEqualTo(bean);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getAlreadyLoaded()).isInstanceOf(HashSet.class);

		assertThat(context.isClusteredEntity()).isTrue();

		Object entityLoader = Whitebox.getInternalState(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(ThriftEntityLoader.class);
	}
}
