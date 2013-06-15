package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static javax.persistence.CascadeType.ALL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.Merger;
import info.archinnov.achilles.proxy.EntityInterceptor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * EntityMergerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class EntityMergerTest
{

	@InjectMocks
	private EntityMerger<AchillesPersistenceContext> entityMerger = new EntityMerger<AchillesPersistenceContext>()
	{};

	@Mock
	private Merger<AchillesPersistenceContext> merger;

	@Mock
	private EntityPersister<AchillesPersistenceContext> persister;

	@Mock
	private EntityProxifier<AchillesPersistenceContext> proxifier;

	@Mock
	private AchillesPersistenceContext context;

	@Mock
	private EntityInterceptor<AchillesPersistenceContext, CompleteBean> interceptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private EntityMeta meta = new EntityMeta();

	private List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

	private Map<Method, PropertyMeta<?, ?>> dirtyMap = new HashMap<Method, PropertyMeta<?, ?>>();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(entityMerger, "merger", merger);
		Whitebox.setInternalState(entityMerger, "persister", persister);
		Whitebox.setInternalState(entityMerger, "proxifier", proxifier);

		when(context.getEntity()).thenReturn(entity);
		when((Class) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(context.addToProcessingList(entity)).thenReturn(true);

		allMetas.clear();
		dirtyMap.clear();
	}

	@Test
	public void should_merge_proxified_entity() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(true);
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(context.addToProcessingList(entity)).thenReturn(true, false);

		PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.accessors()
				.type(JOIN_SIMPLE)
				.cascadeType(ALL)
				.build();

		meta.setAllMetas(Arrays.<PropertyMeta<?, ?>> asList(pm));

		dirtyMap.put(pm.getSetter(), pm);

		CompleteBean actual = entityMerger.merge(context, entity);
		CompleteBean actual2 = entityMerger.merge(context, entity);

		assertThat(actual2).isSameAs(entity);
		assertThat(actual).isSameAs(entity);
		verify(context, times(2)).setEntity(entity);
		verify(merger).merge(context, dirtyMap);
		verify(merger).cascadeMerge(eq(entityMerger), eq(context), any(List.class));

		verify(interceptor).setContext(context);
		verify(interceptor).setTarget(entity);

	}

	@Test
	public void should_persist_transient_entity() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);
		when(context.isWideRow()).thenReturn(false);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = entityMerger.merge(context, entity);

		assertThat(actual).isSameAs(entity);
		verify(persister).persist(context);
	}

	@Test
	public void should_not_persist_transient_wide_row_entity() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);
		when(context.isWideRow()).thenReturn(true);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = entityMerger.merge(context, entity);

		assertThat(actual).isSameAs(entity);
		verifyZeroInteractions(persister);
	}
}
