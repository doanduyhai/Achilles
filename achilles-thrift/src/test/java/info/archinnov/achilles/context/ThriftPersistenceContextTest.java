package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashSet;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.google.common.base.Optional;

/**
 * ThriftPersistenceContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftPersistenceContextTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ThriftPersistenceContext context;

	@Mock
	private MethodInvoker introspector;

	private EntityMeta entityMeta;
	private PropertyMeta<Void, Long> idMeta;

	private EntityMeta joinMeta;
	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private ThriftDaoContext thriftDaoContext;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao wideRowDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private ThriftImmediateFlushContext flushContext;

	@Mock
	private ThriftEntityProxifier proxifier;

	@Mock
	private ThriftEntityPersister persister;

	@Mock
	private ThriftEntityMerger merger;

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private EntityRefresher<ThriftPersistenceContext> refresher;

	private ConfigurationContext configContext = new ConfigurationContext();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private UserBean bean;

	@Before
	public void setUp() throws Exception
	{

		bean = new UserBean();
		bean.setUserId(RandomUtils.nextLong());

		idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.accessors()
				.build();

		configContext.setConsistencyPolicy(policy);
		entityMeta = new EntityMeta();
		entityMeta.setTableName("table");
		entityMeta.setWideRow(false);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setEntityClass(CompleteBean.class);

		when(thriftDaoContext.findEntityDao("table")).thenReturn(entityDao);

		context = new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				flushContext, entity, new HashSet<String>());
	}

	@Test
	public void should_init_with_entity() throws Exception
	{

		assertThat(context.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(context.getEntity()).isEqualTo(entity);
		assertThat(context.getEntityMeta()).isSameAs(entityMeta);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
	}

	@Test
	public void should_init_with_type_and_primary_key() throws Exception
	{
		entityMeta.setWideRow(true);
		when(thriftDaoContext.findWideRowDao("table")).thenReturn(wideRowDao);

		context = new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				flushContext, CompleteBean.class, entity.getId(), new HashSet<String>());

		assertThat(context.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(context.getEntity()).isNull();
		assertThat(context.getEntityMeta()).isSameAs(entityMeta);
		assertThat(context.getWideRowDao()).isSameAs(wideRowDao);
	}

	@Test
	public void should_spawn_child_context_with_join_entity() throws Exception
	{
		prepareJoinContext();

		ThriftPersistenceContext joinContext = context.newPersistenceContext(joinMeta, bean);

		assertThat(joinContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(joinContext.getEntity()).isEqualTo(bean);
		assertThat((Class<UserBean>) joinContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(joinContext.getEntityMeta()).isSameAs(joinMeta);
		assertThat(joinContext.getEntityDao()).isSameAs(entityDao);

	}

	@Test
	public void should_spawn_child_context_with_id() throws Exception
	{
		prepareJoinContext();

		ThriftPersistenceContext joinContext = (ThriftPersistenceContext) context
				.newPersistenceContext(UserBean.class, joinMeta, bean.getUserId());

		assertThat(joinContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(joinContext.getEntity()).isNull();
		assertThat((Class<UserBean>) joinContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(joinContext.getEntityMeta()).isSameAs(joinMeta);
		assertThat(joinContext.getEntityDao()).isSameAs(entityDao);
	}

	@Test
	public void should_persist() throws Exception
	{
		Whitebox.setInternalState(context, "persister", persister);

		context.persist(Optional.<ConsistencyLevel> absent());

		verify(persister).persist(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_persist_with_consistency() throws Exception
	{
		context = new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				flushContext, entity, new HashSet<String>());

		Whitebox.setInternalState(context, "persister", persister);

		context.persist(Optional.fromNullable(EACH_QUORUM));

		verify(policy).setCurrentWriteLevel(EACH_QUORUM);
		verify(persister).persist(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_merge() throws Exception
	{
		Whitebox.setInternalState(context, "merger", merger);
		when(merger.merge(context, entity)).thenReturn(entity);

		CompleteBean merged = context.merge(entity, Optional.<ConsistencyLevel> absent());

		assertThat(merged).isSameAs(entity);
		verify(flushContext).flush();
	}

	@Test
	public void should_merge_with_consistency() throws Exception
	{
		Whitebox.setInternalState(context, "merger", merger);
		when(merger.merge(context, entity)).thenReturn(entity);

		CompleteBean merged = context.merge(entity, Optional.fromNullable(EACH_QUORUM));

		assertThat(merged).isSameAs(entity);
		verify(policy).setCurrentWriteLevel(EACH_QUORUM);
		verify(flushContext).flush();
	}

	@Test
	public void should_remove() throws Exception
	{
		Whitebox.setInternalState(context, "persister", persister);

		context.remove(Optional.<ConsistencyLevel> absent());

		verify(persister).remove(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_remove_with_consistency() throws Exception
	{
		Whitebox.setInternalState(context, "persister", persister);

		context.remove(Optional.fromNullable(EACH_QUORUM));

		verify(policy).setCurrentWriteLevel(EACH_QUORUM);
		verify(persister).remove(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_find() throws Exception
	{
		Whitebox.setInternalState(context, "loader", loader);
		Whitebox.setInternalState(context, "proxifier", proxifier);

		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = context
				.find(CompleteBean.class, Optional.<ConsistencyLevel> absent());

		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_find_with_consistency() throws Exception
	{
		Whitebox.setInternalState(context, "loader", loader);
		Whitebox.setInternalState(context, "proxifier", proxifier);

		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = context.find(CompleteBean.class, Optional.fromNullable(EACH_QUORUM));

		verify(policy).setCurrentReadLevel(EACH_QUORUM);
		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		context = new ThriftPersistenceContext(entityMeta, configContext, thriftDaoContext,
				flushContext, entity, new HashSet<String>());

		Whitebox.setInternalState(context, "loader", loader);
		Whitebox.setInternalState(context, "proxifier", proxifier);

		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = context.getReference(CompleteBean.class,
				Optional.<ConsistencyLevel> absent());

		assertThat(context.isLoadEagerFields()).isFalse();
		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_get_reference_with_consistency() throws Exception
	{
		Whitebox.setInternalState(context, "loader", loader);
		Whitebox.setInternalState(context, "proxifier", proxifier);

		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = context.getReference(CompleteBean.class,
				Optional.fromNullable(EACH_QUORUM));

		assertThat(context.isLoadEagerFields()).isFalse();
		verify(policy).setCurrentReadLevel(EACH_QUORUM);
		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_refresh() throws Exception
	{
		Whitebox.setInternalState(context, "refresher", refresher);

		context.refresh(Optional.<ConsistencyLevel> absent());
		verify(refresher).refresh(context);
	}

	@Test
	public void should_refresh_with_consistency() throws Exception
	{
		Whitebox.setInternalState(context, "refresher", refresher);

		context.refresh(Optional.fromNullable(EACH_QUORUM));
		verify(policy).setCurrentReadLevel(EACH_QUORUM);
		verify(refresher).refresh(context);
	}

	@Test
	public void should_recover_from_exception() throws Exception
	{
		Whitebox.setInternalState(context, "refresher", refresher);
		doThrow(new AchillesException("")).when(refresher).refresh(context);

		try
		{
			context.refresh(Optional.fromNullable(EACH_QUORUM));
		}
		catch (AchillesException e)
		{
			verify(policy).setCurrentReadLevel(EACH_QUORUM);
			verify(policy).reinitCurrentConsistencyLevels();
			verify(policy).reinitDefaultConsistencyLevels();
		}
	}

	@Test
	public void should_find_entity_dao() throws Exception
	{
		assertThat(context.findEntityDao("table")).isSameAs(entityDao);
	}

	@Test
	public void should_find_wide_row_dao() throws Exception
	{
		when(thriftDaoContext.findWideRowDao("table")).thenReturn(wideRowDao);
		assertThat(context.findWideRowDao("table")).isSameAs(wideRowDao);
	}

	@Test
	public void should_get_counter_dao() throws Exception
	{
		ThriftCounterDao counterDao = mock(ThriftCounterDao.class);

		when(thriftDaoContext.getCounterDao()).thenReturn(counterDao);
		assertThat(context.getCounterDao()).isSameAs(counterDao);
	}

	@Test
	public void should_get_entity_mutator() throws Exception
	{
		Mutator<Object> mutator = mock(Mutator.class);

		when(flushContext.getEntityMutator("table")).thenReturn(mutator);
		assertThat(context.getEntityMutator("table")).isSameAs(mutator);
	}

	@Test
	public void should_get_wide_row_mutator() throws Exception
	{
		Mutator<Object> mutator = mock(Mutator.class);

		when(flushContext.getWideRowMutator("table")).thenReturn(mutator);
		assertThat(context.getWideRowMutator("table")).isSameAs(mutator);
	}

	@Test
	public void should_get_counter_mutator() throws Exception
	{
		Mutator<Object> mutator = mock(Mutator.class);

		when(flushContext.getCounterMutator()).thenReturn(mutator);
		assertThat(context.getCounterMutator()).isSameAs(mutator);
	}

	private void prepareJoinContext() throws Exception
	{
		bean = new UserBean();
		bean.setUserId(123L);

		joinIdMeta = PropertyMetaTestBuilder//
				.of(UserBean.class, Void.class, Long.class)
				.field("userId")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		joinMeta = new EntityMeta();

		joinMeta.setTableName("joinTable");
		joinMeta.setWideRow(false);
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setEntityClass(UserBean.class);
		joinMeta.setWideRow(false);

		when(thriftDaoContext.findEntityDao("joinTable")).thenReturn(entityDao);
	}

}
