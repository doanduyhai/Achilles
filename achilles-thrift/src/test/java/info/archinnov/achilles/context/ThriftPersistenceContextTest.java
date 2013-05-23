package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftPersistenceContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftPersistenceContextTest
{
	@Mock
	private AchillesMethodInvoker introspector;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private EntityMeta joinMeta;

	private PropertyMeta<Void, Long> idMeta;

	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private Map<String, ThriftGenericEntityDao> entityDaosMap;

	@Mock
	private ThriftDaoContext thriftDaoContext;

	@Mock
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	private AchillesConfigurationContext configContext = new AchillesConfigurationContext();

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao columFamilyDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private UserBean bean;

	@Before
	public void setUp()
	{
		when(thriftDaoContext.getCounterDao()).thenReturn(thriftCounterDao);
		configContext.setConsistencyPolicy(policy);
	}

	@Test
	public void should_init_entity_dao_on_initialization() throws Exception
	{
		prepareContextWithEntityDao();

		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, thriftImmediateFlushContext, entity);

		assertThat(context.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(context.getEntity()).isEqualTo(entity);
		assertThat((Class<CompleteBean>) context.getEntityClass()).isEqualTo(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
	}

	@Test
	public void should_init_wide_row_dao_on_initialization() throws Exception
	{
		prepareContext();
		when(entityMeta.isWideRow()).thenReturn(true);
		when((ThriftGenericWideRowDao) thriftDaoContext.findWideRowDao("cf")).thenReturn(
				columFamilyDao);

		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, thriftImmediateFlushContext, entity);

		assertThat(context.getColumnFamilyDao()).isSameAs(columFamilyDao);
	}

	@Test
	public void should_init_with_primary_key() throws Exception
	{
		prepareContextWithEntityDao();
		Long primaryKey = 150L;
		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, thriftImmediateFlushContext, CompleteBean.class, primaryKey);

		assertThat(context.getPrimaryKey()).isEqualTo(primaryKey);
		assertThat(context.getEntity()).isNull();
		assertThat((Class<CompleteBean>) context.getEntityClass()).isEqualTo(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
	}

	@Test
	public void should_spawn_child_context() throws Exception
	{
		prepareContextWithEntityDao();
		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, thriftImmediateFlushContext, entity);

		prepareNewContext();

		ThriftPersistenceContext newContext = (ThriftPersistenceContext) context
				.newPersistenceContext(joinMeta, bean);

		assertThat(newContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(newContext.getEntity()).isEqualTo(bean);
		assertThat((Class<UserBean>) newContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(newContext.getEntityDao()).isSameAs(entityDao);

	}

	@Test
	public void should_spawn_child_context_with_id() throws Exception
	{
		prepareContextWithEntityDao();
		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, thriftImmediateFlushContext, entity);

		prepareNewContext();

		ThriftPersistenceContext newContext = (ThriftPersistenceContext) context
				.newPersistenceContext(UserBean.class, joinMeta, bean.getUserId());

		assertThat(newContext.getPrimaryKey()).isEqualTo(bean.getUserId());
		assertThat(newContext.getEntity()).isNull();
		assertThat((Class<UserBean>) newContext.getEntityClass()).isEqualTo(UserBean.class);
		assertThat(newContext.getEntityDao()).isSameAs(entityDao);
	}

	@Test
	public void should_find_wide_row_dao() throws Exception
	{
		prepareContextWithEntityDao();
		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, configContext,
				thriftDaoContext, thriftImmediateFlushContext, entity);

		when(thriftDaoContext.findWideRowDao("cf")).thenReturn(columFamilyDao);

		assertThat((ThriftGenericWideRowDao) context.findWideRowDao("cf")).isSameAs(columFamilyDao);
	}

	// //////////////////
	private void prepareContext() throws Exception
	{
		idMeta = PropertyMetaTestBuilder//
				.of(CompleteBean.class, Void.class, Long.class)
				.field("id")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
		when(introspector.getPrimaryKey(entity, idMeta)).thenReturn(entity.getId());
		when(entityMeta.getTableName()).thenReturn("cf");
	}

	private void prepareContextWithEntityDao() throws Exception
	{
		prepareContext();
		when(entityMeta.isWideRow()).thenReturn(false);
		when(thriftDaoContext.findEntityDao("cf")).thenReturn(entityDao);
	}

	private void prepareNewContext() throws Exception
	{
		bean = new UserBean();
		bean.setUserId(123L);

		joinIdMeta = PropertyMetaTestBuilder//
				.of(UserBean.class, Void.class, Long.class)
				.field("userId")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		when((PropertyMeta<Void, Long>) joinMeta.getIdMeta()).thenReturn(joinIdMeta);
		when(introspector.getPrimaryKey(bean, joinIdMeta)).thenReturn(bean.getUserId());
		when(joinMeta.getTableName()).thenReturn("cf2");
		when(joinMeta.isWideRow()).thenReturn(false);
		when(thriftDaoContext.findEntityDao("cf2")).thenReturn(entityDao);
	}

}
