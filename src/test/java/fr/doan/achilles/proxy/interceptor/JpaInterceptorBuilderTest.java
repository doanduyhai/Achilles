package fr.doan.achilles.proxy.interceptor;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;

@RunWith(MockitoJUnitRunner.class)
public class JpaInterceptorBuilderTest
{
	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private GenericEntityDao<Long> dao;

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
	public void should_build() throws Exception
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

		JpaEntityInterceptor<Long> interceptor = JpaInterceptorBuilder.builder(entityMeta).target(entity)
				.build();

		assertThat(interceptor.getKey()).isEqualTo(1L);
		assertThat(interceptor.getTarget()).isEqualTo(entity);
		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isInstanceOf(HashMap.class);

		assertThat(interceptor.getLazyLoaded()).isNotNull();
		assertThat(interceptor.getLazyLoaded()).isInstanceOf(HashSet.class);

		assertThat(interceptor.getKey()).isEqualTo(1L);

		Object entityLoader = ReflectionTestUtils.getField(interceptor, "loader");

		assertThat(entityLoader).isNotNull();
		assertThat(entityLoader).isInstanceOf(EntityLoader.class);
	}

}
