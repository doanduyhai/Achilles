package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * PersistenceContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class PersistenceContextTest
{
	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private EntityMeta<Long> joinMeta;

	private PropertyMeta<Void, Long> idMeta;

	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;

	@Mock
	private Map<String, GenericCompositeDao<?, ?>> columnFamilyDaosMap;

	@Mock
	private CounterDao counterDao;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Mock
	private GenericDynamicCompositeDao<Long> entityDao;

	@Mock
	private GenericCompositeDao<Long, String> columFamilyDao;

	@Mock
	private Mutator<Long> mutator;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private UserBean bean;

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_entity_dao_on_initialization() throws Exception
	{
		prepareContextWithEntityDao();

		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		assertThat(context.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(context.getEntity()).isEqualTo(entity);
		assertThat((Class<CompleteBean>) context.getEntityClass()).isEqualTo(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_column_family_dao_on_initialization() throws Exception
	{
		prepareContext();
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);
		when((GenericCompositeDao<Long, String>) columnFamilyDaosMap.get("cf")).thenReturn(
				columFamilyDao);

		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		assertThat((GenericCompositeDao<Long, String>) context.getColumnFamilyDao()).isSameAs(
				columFamilyDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_with_primary_key() throws Exception
	{
		prepareContextWithEntityDao();
		Long primaryKey = 150L;
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, CompleteBean.class, primaryKey);

		assertThat(context.getPrimaryKey()).isEqualTo(primaryKey);
		assertThat(context.getEntity()).isNull();
		assertThat((Class<CompleteBean>) context.getEntityClass()).isEqualTo(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_spawn_child_context() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		prepareNewContext();

		PersistenceContext<Long> newContext = context.newPersistenceContext(joinMeta, bean);

		assertThat(newContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(newContext.getEntity()).isEqualTo(bean);
		assertThat((Class<UserBean>) newContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(newContext.getEntityDao()).isSameAs(entityDao);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_spawn_child_context_with_id() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		prepareNewContext();

		PersistenceContext<Long> newContext = context.newPersistenceContext(UserBean.class,
				joinMeta, bean.getUserId());

		assertThat(newContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(newContext.getEntity()).isNull();
		assertThat((Class<UserBean>) newContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(newContext.getEntityDao()).isSameAs(entityDao);
	}

	@Test
	public void should_start_batch() throws Exception
	{
		prepareContextWithEntityDao();
		when(entityDao.buildMutator()).thenReturn(mutator);

		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		context.startBatch();

		assertThat(context.getMutator()).isSameAs(mutator);
	}

	@Test
	public void should_not_start_batch() throws Exception
	{
		prepareContextWithEntityDao();
		when(entityDao.buildMutator()).thenReturn(mutator);

		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		Whitebox.setInternalState(context, "pendingBatch", true);
		context.startBatch();

		assertThat(context.getMutator()).isNull();
	}

	@Test
	public void should_end_batch() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);
		Whitebox.setInternalState(context, "mutator", mutator);

		context.endBatch();

		verify(entityDao).executeMutator(mutator);
		assertThat(context.getMutator()).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_start_batch_for_join() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		when((GenericDynamicCompositeDao<Long>) entityDaosMap.get("cf")).thenReturn(entityDao);
		when(entityDao.buildMutator()).thenReturn(mutator);

		context.startBatchForJoin("cf");
		assertThat((Mutator<Long>) context.getJoinMutator()).isSameAs(mutator);
	}

	@Test
	public void should_end_batch_for_join() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		Whitebox.setInternalState(context, "joinMutator", mutator);
		Whitebox.setInternalState(context, "joinEntityDao", entityDao);

		context.endBatchForJoin();

		verify(entityDao).executeMutator(mutator);

		assertThat(context.getJoinMutator()).isNull();
		assertThat(Whitebox.getInternalState(context, "joinEntityDao")).isNull();
	}

	@Test
	public void should_not_end_batch() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);
		Whitebox.setInternalState(context, "pendingBatch", true);
		Whitebox.setInternalState(context, "mutator", mutator);

		context.endBatch();

		verifyZeroInteractions(entityDao);
		assertThat(context.getMutator()).isSameAs(mutator);

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_column_family_dao() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, entityDaosMap,
				columnFamilyDaosMap, counterDao, policy, entity);

		when((GenericCompositeDao<Long, String>) columnFamilyDaosMap.get("cf")).thenReturn(
				columFamilyDao);

		assertThat((GenericCompositeDao) context.findColumnFamilyDao("cf"))
				.isSameAs(columFamilyDao);
	}

	// //////////////////
	private void prepareContext() throws Exception
	{
		idMeta = PropertyMetaTestBuilder//
				.of(CompleteBean.class, Void.class, Long.class) //
				.field("id") //
				.accesors() //
				.type(PropertyType.SIMPLE) //
				.build();

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(introspector.getKey(entity, idMeta)).thenReturn(entity.getId());
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
	}

	@SuppressWarnings("unchecked")
	private void prepareContextWithEntityDao() throws Exception
	{
		prepareContext();
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(false);
		when((GenericDynamicCompositeDao<Long>) entityDaosMap.get("cf")).thenReturn(entityDao);
	}

	@SuppressWarnings("unchecked")
	private void prepareNewContext() throws Exception
	{
		bean = new UserBean();
		bean.setUserId(123L);

		joinIdMeta = PropertyMetaTestBuilder//
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId") //
				.accesors() //
				.type(PropertyType.SIMPLE) //
				.build();

		when(joinMeta.getIdMeta()).thenReturn(joinIdMeta);
		when(introspector.getKey(bean, joinIdMeta)).thenReturn(bean.getUserId());
		when(joinMeta.getColumnFamilyName()).thenReturn("cf2");
		when(joinMeta.isColumnFamilyDirectMapping()).thenReturn(false);
		when((GenericDynamicCompositeDao<Long>) entityDaosMap.get("cf2")).thenReturn(entityDao);

	}
}
