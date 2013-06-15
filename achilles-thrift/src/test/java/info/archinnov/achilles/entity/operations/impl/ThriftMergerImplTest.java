package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.proxy.MethodInvoker;

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

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.ImmutableMap;

/**
 * ThriftMergerImplTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftMergerImplTest
{

	@InjectMocks
	private ThriftMergerImpl mergerImpl;

	@Mock
	private ThriftEntityPersister persister;

	@Mock
	private MethodInvoker invoker;

	@Mock
	private ThriftEntityMerger entityMerger;

	@Mock
	private ThriftPersistenceContext context;

	@Mock
	private ThriftPersistenceContext joinContext;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private UserBean user = new UserBean();

	private EntityMeta joinMeta = new EntityMeta();

	private Map<Method, PropertyMeta<?, ?>> dirtyMap = new HashMap<Method, PropertyMeta<?, ?>>();

	private List<PropertyMeta<?, ?>> joinPMs = new ArrayList<PropertyMeta<?, ?>>();

	@Before
	public void setUp()
	{
		when(context.getEntity()).thenReturn(entity);
		dirtyMap.clear();
		joinPMs.clear();
	}

	@Test
	public void should_merge_simple_property() throws Exception
	{
		PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(SIMPLE)
				.build();

		dirtyMap.put(pm.getSetter(), pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn("name");

		mergerImpl.merge(context, dirtyMap);

		verify(persister).persistPropertyBatch(context, pm);
	}

	@Test
	public void should_merge_multi_values_property() throws Exception
	{
		PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("friends")
				.accessors()
				.type(LIST)
				.build();

		dirtyMap.put(pm.getSetter(), pm);

		when(invoker.getValueFromField(entity, pm.getGetter()))
				.thenReturn(Arrays.asList("friends"));

		mergerImpl.merge(context, dirtyMap);

		verify(persister).removePropertyBatch(context, pm);
		verify(persister).persistPropertyBatch(context, pm);
	}

	@Test
	public void should_remove_property_when_null() throws Exception
	{
		PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(SIMPLE)
				.build();

		dirtyMap.put(pm.getSetter(), pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(null);

		mergerImpl.merge(context, dirtyMap);

		verify(persister).removePropertyBatch(context, pm);
		verify(persister, never()).persistPropertyBatch(context, pm);
	}

	@Test
	public void should_do_nothing_when_not_dirty() throws Exception
	{
		mergerImpl.merge(context, dirtyMap);

		verifyZeroInteractions(context, invoker, persister);
	}

	@Test
	public void should_cascade_merge_simple_property() throws Exception
	{
		PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.accessors()
				.type(JOIN_SIMPLE)
				.joinMeta(joinMeta)
				.build();

		joinPMs.add(pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(user);
		when(context.newPersistenceContext(joinMeta, user)).thenReturn(joinContext);

		mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

		verify(entityMerger).merge(joinContext, user);
	}

	@Test
	public void should_cascade_merge_collection_property() throws Exception
	{
		PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.accessors()
				.type(JOIN_LIST)
				.joinMeta(joinMeta)
				.build();

		joinPMs.add(pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(
				Arrays.asList(user, null));
		when(context.newPersistenceContext(joinMeta, user)).thenReturn(joinContext);

		mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

		verify(entityMerger).merge(joinContext, user);
	}

	@Test
	public void should_cascade_merge_map_property() throws Exception
	{
		PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.accessors()
				.type(JOIN_MAP)
				.joinMeta(joinMeta)
				.build();

		joinPMs.add(pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(
				ImmutableMap.of(11, user));
		when(context.newPersistenceContext(joinMeta, user)).thenReturn(joinContext);

		mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

		verify(entityMerger).merge(joinContext, user);
	}

	@Test
	public void should_not_cascade_merge_for_null_value() throws Exception
	{
		PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.accessors()
				.type(JOIN_LIST)
				.joinMeta(joinMeta)
				.build();

		joinPMs.add(pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(null);

		mergerImpl.cascadeMerge(entityMerger, context, joinPMs);

		verifyZeroInteractions(entityMerger);
		verify(context, never()).newPersistenceContext(eq(joinMeta), any());

	}
}
