package info.archinnov.achilles.entity.operations.impl;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLAbstractFlushContext;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.BoundStatement;
import com.google.common.collect.ImmutableMap;

/**
 * CQLPersisterImplTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPersisterImplTest
{
	@InjectMocks
	private CQLPersisterImpl persisterImpl = new CQLPersisterImpl();

	@Mock
	private AchillesMethodInvoker invoker;

	@Mock
	private CQLEntityPersister entityPersister;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private CQLPersistenceContext joinContext;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private EntityMeta joinMeta;

	private List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	@Before
	public void setUp()
	{
		when(context.getEntity()).thenReturn(entity);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getDaoContext()).thenReturn(daoContext);

		when(entityMeta.getAllMetas()).thenReturn(allMetas);
		allMetas.clear();
	}

	@Test
	public void should_persist() throws Exception
	{

		BoundStatement boundStatement = mock(BoundStatement.class);
		when(daoContext.bindForInsert(context)).thenReturn(boundStatement);

		CQLAbstractFlushContext flushContext = mock(CQLAbstractFlushContext.class);
		when(context.getFlushContext()).thenReturn(flushContext);

		persisterImpl.persist(entityPersister, context);

		verify(flushContext).pushBoundStatement(boundStatement, entityMeta);

	}

	@Test
	public void should_cascade_to_join_simple() throws Exception
	{
		PropertyMeta<?, ?> joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.JOIN_SIMPLE)
				.joinMeta(joinMeta)
				.cascadeType(CascadeType.ALL)
				.build();

		allMetas.add(joinSimpleMeta);

		Object user = new UserBean();
		when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(user);

		when(context.newPersistenceContext(joinMeta, user)).thenReturn(joinContext);

		persisterImpl.cascadePersist(entityPersister, context);

		verify(entityPersister).cascadePersistOrEnsureExist(joinContext,
				joinSimpleMeta.getJoinProperties());
	}

	@Test
	public void should_cascade_to_join_collection() throws Exception
	{
		PropertyMeta<?, ?> joinListMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.JOIN_LIST)
				.cascadeType(CascadeType.ALL)
				.joinMeta(joinMeta)
				.build();

		allMetas.add(joinListMeta);

		UserBean user1 = new UserBean();
		UserBean user2 = new UserBean();
		List<UserBean> users = Arrays.asList(user1, user2);
		when(invoker.getValueFromField(entity, joinListMeta.getGetter())).thenReturn(users);

		when(context.newPersistenceContext(joinMeta, user1)).thenReturn(joinContext);
		when(context.newPersistenceContext(joinMeta, user2)).thenReturn(joinContext);

		persisterImpl.cascadePersist(entityPersister, context);

		verify(entityPersister, times(2)).cascadePersistOrEnsureExist(joinContext,
				joinListMeta.getJoinProperties());
	}

	@Test
	public void should_cascade_to_join_map() throws Exception
	{
		PropertyMeta<?, ?> joinMapMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.JOIN_MAP)
				.cascadeType(CascadeType.ALL)
				.joinMeta(joinMeta)
				.build();

		allMetas.add(joinMapMeta);

		UserBean user1 = new UserBean();
		UserBean user2 = new UserBean();
		Map<Integer, UserBean> users = ImmutableMap.of(1, user1, 2, user2);
		when(invoker.getValueFromField(entity, joinMapMeta.getGetter())).thenReturn(users);

		when(context.newPersistenceContext(joinMeta, user1)).thenReturn(joinContext);
		when(context.newPersistenceContext(joinMeta, user2)).thenReturn(joinContext);

		persisterImpl.cascadePersist(entityPersister, context);

		verify(entityPersister, times(2)).cascadePersistOrEnsureExist(joinContext,
				joinMapMeta.getJoinProperties());
	}

	@Test
	public void should_not_cascade_if_null() throws Exception
	{
		PropertyMeta<?, ?> joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.JOIN_SIMPLE)
				.joinMeta(joinMeta)
				.build();

		allMetas.add(joinSimpleMeta);

		persisterImpl.cascadePersist(entityPersister, context);

		verifyZeroInteractions(entityPersister);
	}

	@Test
	public void should_not_cascade_persist_join_widemap() throws Exception
	{
		PropertyMeta<?, ?> joinWideMapMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.JOIN_WIDE_MAP)
				.joinMeta(joinMeta)
				.build();

		allMetas.add(joinWideMapMeta);

		persisterImpl.cascadePersist(entityPersister, context);

		verifyZeroInteractions(entityPersister);
	}

	@Test
	public void should_not_cascade_persist_widemap() throws Exception
	{
		PropertyMeta<?, ?> wideMapMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.WIDE_MAP)
				.build();

		allMetas.add(wideMapMeta);

		persisterImpl.cascadePersist(entityPersister, context);

		verifyZeroInteractions(entityPersister);
	}

	@Test
	public void should_check_for_entity_existence() throws Exception
	{

		when(daoContext.checkForEntityExistence(context)).thenReturn(true);

		boolean actual = persisterImpl.doesEntityExist(context);

		assertThat(actual).isTrue();
	}

	@Test
	public void should_remove() throws Exception
	{
		when(entityMeta.getTableName()).thenReturn("table");

		BoundStatement boundStatement = mock(BoundStatement.class);
		when(daoContext.bindForRemove(context, "table")).thenReturn(boundStatement);

		CQLAbstractFlushContext flushContext = mock(CQLAbstractFlushContext.class);
		when(context.getFlushContext()).thenReturn(flushContext);

		persisterImpl.remove(context);

		verify(flushContext).pushBoundStatement(boundStatement, entityMeta);
	}

	@Test
	public void should_remove_linked_tables() throws Exception
	{
		PropertyMeta<?, ?> wideMapMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class)
				.field("user")
				.type(PropertyType.WIDE_MAP)
				.externalTable("external_table")
				.build();

		allMetas.add(wideMapMeta);

		CQLAbstractFlushContext flushContext = mock(CQLAbstractFlushContext.class);
		when(context.getFlushContext()).thenReturn(flushContext);

		BoundStatement boundStatement = mock(BoundStatement.class);
		when(daoContext.bindForRemove(context, "external_table")).thenReturn(boundStatement);

		persisterImpl.removeLinkedTables(context);

		verify(flushContext).pushBoundStatement(boundStatement, wideMapMeta);
	}
}
