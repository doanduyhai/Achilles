package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
	private Map<String, GenericEntityDao<?>> entityDaosMap;

	@Mock
	private DaoContext daoContext;

	@Mock
	private Map<String, GenericWideRowDao<?, ?>> columnFamilyDaosMap;

	@Mock
	private CounterDao counterDao;

	private ConfigurationContext configContext = new ConfigurationContext();

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Mock
	private GenericEntityDao<Long> entityDao;

	@Mock
	private GenericWideRowDao<Long, String> columFamilyDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private ImmediateFlushContext immediateFlushContext;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private UserBean bean;

	@Before
	public void setUp()
	{
		when(daoContext.getCounterDao()).thenReturn(counterDao);
		configContext.setConsistencyPolicy(policy);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_entity_dao_on_initialization() throws Exception
	{
		prepareContextWithEntityDao();

		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, configContext,
				daoContext, immediateFlushContext, entity);

		assertThat(context.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(context.getEntity()).isEqualTo(entity);
		assertThat((Class<CompleteBean>) context.getEntityClass()).isEqualTo(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_wide_row_dao_on_initialization() throws Exception
	{
		prepareContext();
		when(entityMeta.isWideRow()).thenReturn(true);
		when((GenericWideRowDao<Long, String>) daoContext.findWideRowDao("cf")).thenReturn(
				columFamilyDao);

		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, configContext,
				daoContext, immediateFlushContext, entity);

		assertThat((GenericWideRowDao<Long, String>) context.getColumnFamilyDao()).isSameAs(
				columFamilyDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_with_primary_key() throws Exception
	{
		prepareContextWithEntityDao();
		Long primaryKey = 150L;
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, configContext,
				daoContext, immediateFlushContext, CompleteBean.class, primaryKey);

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
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, configContext,
				daoContext, immediateFlushContext, entity);

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
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, configContext,
				daoContext, immediateFlushContext, entity);

		prepareNewContext();

		PersistenceContext<Long> newContext = context.newPersistenceContext(UserBean.class,
				joinMeta, bean.getUserId());

		assertThat(newContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(newContext.getEntity()).isNull();
		assertThat((Class<UserBean>) newContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(newContext.getEntityDao()).isSameAs(entityDao);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_wide_row_dao() throws Exception
	{
		prepareContextWithEntityDao();
		PersistenceContext<Long> context = new PersistenceContext<Long>(entityMeta, configContext,
				daoContext, immediateFlushContext, entity);

		when((GenericWideRowDao<Long, String>) daoContext.findWideRowDao("cf")).thenReturn(
				columFamilyDao);

		assertThat((GenericWideRowDao) context.findWideRowDao("cf")).isSameAs(columFamilyDao);
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
		when(entityMeta.isWideRow()).thenReturn(false);
		when((GenericEntityDao<Long>) daoContext.findEntityDao("cf")).thenReturn(entityDao);
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
		when(joinMeta.isWideRow()).thenReturn(false);
		when((GenericEntityDao<Long>) daoContext.findEntityDao("cf2")).thenReturn(entityDao);
	}

}
