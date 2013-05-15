package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
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
public class ThriftPersistenceContextTest
{
	@Mock
	private AchillesEntityIntrospector introspector;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private EntityMeta<Long> joinMeta;

	private PropertyMeta<Void, Long> idMeta;

	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private Map<String, ThriftGenericEntityDao<?>> entityDaosMap;

	@Mock
	private DaoContext daoContext;

	@Mock
	private Map<String, ThriftGenericWideRowDao<?, ?>> columnFamilyDaosMap;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	private AchillesConfigurationContext configContext = new AchillesConfigurationContext();

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private ThriftGenericEntityDao<Long> entityDao;

	@Mock
	private ThriftGenericWideRowDao<Long, String> columFamilyDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private UserBean bean;

	@Before
	public void setUp()
	{
		when(daoContext.getCounterDao()).thenReturn(thriftCounterDao);
		configContext.setConsistencyPolicy(policy);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_entity_dao_on_initialization() throws Exception
	{
		prepareContextWithEntityDao();

		ThriftPersistenceContext<Long> context = new ThriftPersistenceContext<Long>(entityMeta, configContext,
				daoContext, thriftImmediateFlushContext, entity);

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
		when((ThriftGenericWideRowDao<Long, String>) daoContext.findWideRowDao("cf")).thenReturn(
				columFamilyDao);

		ThriftPersistenceContext<Long> context = new ThriftPersistenceContext<Long>(entityMeta, configContext,
				daoContext, thriftImmediateFlushContext, entity);

		assertThat((ThriftGenericWideRowDao<Long, String>) context.getColumnFamilyDao()).isSameAs(
				columFamilyDao);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_init_with_primary_key() throws Exception
	{
		prepareContextWithEntityDao();
		Long primaryKey = 150L;
		ThriftPersistenceContext<Long> context = new ThriftPersistenceContext<Long>(entityMeta, configContext,
				daoContext, thriftImmediateFlushContext, CompleteBean.class, primaryKey);

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
		ThriftPersistenceContext<Long> context = new ThriftPersistenceContext<Long>(entityMeta, configContext,
				daoContext, thriftImmediateFlushContext, entity);

		prepareNewContext();

		ThriftPersistenceContext<Long> newContext = context.newPersistenceContext(joinMeta, bean);

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
		ThriftPersistenceContext<Long> context = new ThriftPersistenceContext<Long>(entityMeta, configContext,
				daoContext, thriftImmediateFlushContext, entity);

		prepareNewContext();

		ThriftPersistenceContext<Long> newContext = context.newPersistenceContext(UserBean.class,
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
		ThriftPersistenceContext<Long> context = new ThriftPersistenceContext<Long>(entityMeta, configContext,
				daoContext, thriftImmediateFlushContext, entity);

		when((ThriftGenericWideRowDao<Long, String>) daoContext.findWideRowDao("cf")).thenReturn(
				columFamilyDao);

		assertThat((ThriftGenericWideRowDao) context.findWideRowDao("cf")).isSameAs(columFamilyDao);
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
		when((ThriftGenericEntityDao<Long>) daoContext.findEntityDao("cf")).thenReturn(entityDao);
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
		when((ThriftGenericEntityDao<Long>) daoContext.findEntityDao("cf2")).thenReturn(entityDao);
	}

}
