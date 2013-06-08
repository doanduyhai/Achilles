package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;

import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private ThriftEntityManager em;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

	@Mock
	private Map<String, ThriftGenericEntityDao> entityDaosMap;

	@Mock
	private EntityMeta entityMeta;

	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftEntityProxifier proxifier;

	@Mock
	private ThriftDaoContext thriftDaoContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private ThriftConsistencyLevelPolicy consistencyPolicy;

	private Long primaryKey = 1165446L;
	private CompleteBean entity = CompleteBeanTestBuilder
			.builder()
			.id(primaryKey)
			.name("name")
			.buid();

	@Before
	public void setUp() throws Exception
	{
		doCallRealMethod().when(em).setConsistencyPolicy(consistencyPolicy);
		em.setConsistencyPolicy(consistencyPolicy);
	}

	@Test
	public void should_init_persistence_context_with_class_and_primary_key() throws Exception
	{
		prepareDataForPersistenceContext();
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(thriftDaoContext.findEntityDao("table")).thenReturn(entityDao);

		doCallRealMethod().when(em).initPersistenceContext(CompleteBean.class, entity.getId());

		ThriftPersistenceContext context = em.initPersistenceContext(CompleteBean.class,
				entity.getId());

		assertThat(context.getEntityMeta()).isSameAs(entityMeta);
		assertThat(context.getConfigContext()).isSameAs(configContext);
		assertThat(context.getEntity()).isNull();
		assertThat(context.getPrimaryKey()).isSameAs(entity.getId());
		assertThat((Class<CompleteBean>) context.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
		assertThat(context.getPolicy()).isSameAs(consistencyPolicy);
		assertThat(context.isBatchMode()).isFalse();
	}

	@Test
	public void should_init_persistence_context_with_entity() throws Exception
	{
		prepareDataForPersistenceContext();
		when((Class<CompleteBean>) proxifier.deriveBaseClass(entity))
				.thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(thriftDaoContext.findEntityDao("table")).thenReturn(entityDao);

		doCallRealMethod().when(em).initPersistenceContext(entity);

		ThriftPersistenceContext context = em.initPersistenceContext(entity);

		assertThat(context.getEntityMeta()).isSameAs(entityMeta);
		assertThat(context.getConfigContext()).isSameAs(configContext);
		assertThat(context.getEntity()).isSameAs(entity);
		assertThat(context.getPrimaryKey()).isSameAs(entity.getId());
		assertThat((Class<CompleteBean>) context.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(context.getEntityDao()).isSameAs(entityDao);
		assertThat(context.getPolicy()).isSameAs(consistencyPolicy);
		assertThat(context.isBatchMode()).isFalse();
	}

	private void prepareDataForPersistenceContext() throws Exception
	{
		doCallRealMethod().when(em).setProxifier(proxifier);
		em.setProxifier(proxifier);

		doCallRealMethod().when(em).setEntityMetaMap(entityMetaMap);
		em.setEntityMetaMap(entityMetaMap);

		doCallRealMethod().when(em).setConfigContext(configContext);
		em.setConfigContext(configContext);

		doCallRealMethod().when(em).setThriftDaoContext(thriftDaoContext);
		em.setThriftDaoContext(thriftDaoContext);

		idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
		when((Class<Long>) entityMeta.getIdClass()).thenReturn(Long.class);
		when((Class) entityMeta.getEntityClass()).thenReturn(CompleteBean.class);
		when(entityMeta.getTableName()).thenReturn("table");
		when(entityMeta.isWideRow()).thenReturn(false);
		when(configContext.getConsistencyPolicy()).thenReturn(consistencyPolicy);
	}
}
