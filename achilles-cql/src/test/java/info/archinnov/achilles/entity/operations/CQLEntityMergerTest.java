package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLMergerImpl;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * CQLEntityMergerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityMergerTest
{

	@InjectMocks
	private CQLEntityMerger merger;

	@Mock
	private CQLEntityProxifier proxifier;

	@Mock
	private CQLEntityPersister persister;

	@Mock
	private CQLMergerImpl mergerImpl;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private CQLEntityInterceptor<CompleteBean> interceptor;

	@Captor
	private ArgumentCaptor<List<PropertyMeta<?, ?>>> pmCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp()
	{
		when(context.getEntityMeta()).thenReturn(entityMeta);
	}

	@Test
	public void should_merge_entity() throws Exception
	{
		Map<Method, PropertyMeta<?, ?>> dirtyMap = new HashMap<Method, PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(SIMPLE)
				.accessors()
				.build();

		PropertyMeta<?, ?> joinSimpleMeta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.field("joinSimple")
				.type(JOIN_SIMPLE)
				.cascadeType(ALL)
				.build();

		PropertyMeta<?, ?> joinListMeta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.field("joinList")
				.type(JOIN_LIST)
				.cascadeType(PERSIST)
				.build();

		PropertyMeta<?, ?> joinWideMapMeta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.field("joinWideMap")
				.type(JOIN_WIDE_MAP)
				.cascadeType(ALL)
				.build();

		List<PropertyMeta<?, ?>> allMetas = Arrays.asList(idMeta, joinSimpleMeta, joinListMeta,
				joinWideMapMeta);

		when(proxifier.isProxy(entity)).thenReturn(true);
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(entityMeta.getAllMetas()).thenReturn(allMetas);

		CompleteBean actual = merger.merge(context, entity);

		verify(mergerImpl).merge(context, dirtyMap);
		verify(mergerImpl).cascadeMerge(eq(merger), eq(context), pmCaptor.capture());

		assertThat(pmCaptor.getValue()).containsExactly(joinSimpleMeta);

		verify(interceptor).setContext(context);
		verify(interceptor).setTarget(entity);

		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_persist_instead_of_merging() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);
		when(context.isWideRow()).thenReturn(false);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = merger.merge(context, entity);

		verify(persister).persist(context);
		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_not_persist_if_wide_row() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);
		when(context.isWideRow()).thenReturn(true);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = merger.merge(context, entity);

		verifyZeroInteractions(persister);
		assertThat(actual).isSameAs(entity);
	}

}
