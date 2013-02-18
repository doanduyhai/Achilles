package info.archinnov.achilles.proxy.interceptor;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private GenericCompositeDao<Long, String> columnFamilyDao;

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

	@Test
	public void should_build_entity() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		Method idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		JpaEntityInterceptor<Long> interceptor = JpaEntityInterceptorBuilder.builder(entityMeta)
				.target(entity).build();

		assertThat(interceptor.getKey()).isEqualTo(1L);
		assertThat(interceptor.getTarget()).isEqualTo(entity);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getLazyLoaded()).isNotNull();
		assertThat(interceptor.getLazyLoaded()).isInstanceOf(HashSet.class);

		assertThat(interceptor.columnFamilyDao).isNull();
		assertThat(interceptor.entityDao).isNotNull();
		assertThat(interceptor.getDirectColumnFamilyMapping()).isFalse();

		Object entityLoader = interceptor.loader;

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(EntityLoader.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_build_column_family() throws Exception
	{
		ColumnFamilyBean entity = new ColumnFamilyBean();
		entity.setId(1545L);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getColumnFamilyDao()).thenReturn((GenericCompositeDao) columnFamilyDao);

		Method idGetter = ColumnFamilyBean.class.getDeclaredMethod("getId");
		Method idSetter = ColumnFamilyBean.class.getDeclaredMethod("setId", Long.class);

		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(PropertyType.SIMPLE);

		idMeta.setGetter(idGetter);
		idMeta.setSetter(idSetter);

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);

		JpaEntityInterceptor<Long> interceptor = JpaEntityInterceptorBuilder.builder(entityMeta)
				.target(entity).build();

		assertThat(interceptor.getKey()).isEqualTo(1545L);
		assertThat(interceptor.getTarget()).isEqualTo(entity);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getLazyLoaded()).isNotNull();
		assertThat(interceptor.getLazyLoaded()).isInstanceOf(HashSet.class);

		assertThat(interceptor.columnFamilyDao).isNotNull();
		assertThat(interceptor.entityDao).isNull();
		assertThat(interceptor.getDirectColumnFamilyMapping()).isTrue();

		Object entityLoader = interceptor.loader;

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(EntityLoader.class);
	}

}
