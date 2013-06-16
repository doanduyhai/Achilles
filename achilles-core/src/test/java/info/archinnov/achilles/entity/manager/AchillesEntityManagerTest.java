package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;

import com.google.common.base.Optional;

/**
 * AchillesEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class AchillesEntityManagerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private AchillesEntityManager<AchillesPersistenceContext> em;

	@Mock
	private EntityPersister<AchillesPersistenceContext> persister;

	@Mock
	private EntityLoader<AchillesPersistenceContext> loader;

	@Mock
	private EntityMerger<AchillesPersistenceContext> merger;

	@Mock
	private EntityRefresher<AchillesPersistenceContext> refresher;

	@Mock
	private EntityInitializer initializer;

	@Mock
	private EntityProxifier<AchillesPersistenceContext> proxifier;

	@Mock
	private EntityValidator<AchillesPersistenceContext> entityValidator;

	@Mock
	private AchillesPersistenceContext context;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

	private Long primaryKey = 1165446L;
	private CompleteBean entity = CompleteBeanTestBuilder
			.builder()
			.id(primaryKey)
			.name("name")
			.buid();

	private Optional<ConsistencyLevel> noConsistency = Optional.<ConsistencyLevel> absent();

	@Mock
	private EntityMeta entityMeta;

	@Captor
	ArgumentCaptor<Optional<ConsistencyLevel>> levelOCaptor;

	@Before
	public void setUp() throws Exception
	{

		forceMethodCallsOnMock();

		when(em.initPersistenceContext(entity)).thenReturn(context);
		when(em.initPersistenceContext(CompleteBean.class, primaryKey)).thenReturn(context);
	}

	@Test
	public void should_persist() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);
		doCallRealMethod().when(em).persist(entity);
		doCallRealMethod().when(em).persist(entity, noConsistency);

		em.persist(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);
		verify(context).persist(levelOCaptor.capture());

		assertThat(levelOCaptor.getValue().isPresent()).isFalse();
	}

	@Test
	public void should_persistwith_consistency() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);
		doCallRealMethod().when(em).persist(entity, EACH_QUORUM);
		doCallRealMethod().when(em).persist(eq(entity), any(Optional.class));

		em.persist(entity, EACH_QUORUM);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);
		verify(context).persist(levelOCaptor.capture());

		assertThat(levelOCaptor.getValue().get()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(true);
		doCallRealMethod().when(em).persist(entity);
		doCallRealMethod().when(em).persist(entity, noConsistency);

		exception.expect(IllegalStateException.class);

		em.persist(entity);
	}

	@Test
	public void should_merge() throws Exception
	{

		when(context.merge(eq(entity), levelOCaptor.capture())).thenReturn(entity);
		doCallRealMethod().when(em).merge(entity);
		doCallRealMethod().when(em).merge(entity, noConsistency);

		CompleteBean mergedEntity = em.merge(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);

		assertThat(mergedEntity).isSameAs(entity);
		assertThat(levelOCaptor.getValue().isPresent()).isFalse();
	}

	@Test
	public void should_merge_with_consistency() throws Exception
	{
		when(context.merge(eq(entity), levelOCaptor.capture())).thenReturn(entity);
		doCallRealMethod().when(em).merge(entity, EACH_QUORUM);
		doCallRealMethod().when(em).merge(eq(entity), any(Optional.class));

		CompleteBean mergedEntity = em.merge(entity, EACH_QUORUM);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);

		assertThat(mergedEntity).isSameAs(entity);
		assertThat(levelOCaptor.getValue().get()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_remove() throws Exception
	{
		doCallRealMethod().when(em).remove(entity);
		doCallRealMethod().when(em).remove(eq(entity), any(Optional.class));

		em.remove(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(context).remove(levelOCaptor.capture());
		assertThat(levelOCaptor.getValue().isPresent()).isFalse();
	}

	@Test
	public void should_remove_with_consistency() throws Exception
	{
		doCallRealMethod().when(em).remove(entity, EACH_QUORUM);
		doCallRealMethod().when(em).remove(eq(entity), any(Optional.class));

		em.remove(entity, EACH_QUORUM);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(context).remove(levelOCaptor.capture());
		assertThat(levelOCaptor.getValue().get()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_find() throws Exception
	{
		doCallRealMethod().when(em).find(CompleteBean.class, primaryKey);
		doCallRealMethod().when(em).find(CompleteBean.class, primaryKey, noConsistency);

		when(context.find(eq(CompleteBean.class), levelOCaptor.capture())).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, primaryKey);

		assertThat(bean).isSameAs(entity);
		assertThat(levelOCaptor.getValue().isPresent()).isFalse();
	}

	@Test
	public void should_find_with_consistency() throws Exception
	{
		doCallRealMethod().when(em).find(CompleteBean.class, primaryKey, EACH_QUORUM);
		doCallRealMethod().when(em).find(eq(CompleteBean.class), eq(primaryKey),
				any(Optional.class));

		when(context.find(eq(CompleteBean.class), levelOCaptor.capture())).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, primaryKey, EACH_QUORUM);

		assertThat(bean).isSameAs(entity);
		assertThat(levelOCaptor.getValue().get()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(context.getReference(eq(CompleteBean.class), levelOCaptor.capture())).thenReturn(
				entity);
		doCallRealMethod().when(em).getReference(CompleteBean.class, primaryKey);
		doCallRealMethod().when(em).getReference(CompleteBean.class, primaryKey, noConsistency);

		CompleteBean bean = em.getReference(CompleteBean.class, primaryKey);

		assertThat(bean).isSameAs(entity);
		assertThat(levelOCaptor.getValue().isPresent()).isFalse();
	}

	@Test
	public void should_get_reference_with_consistency() throws Exception
	{
		when(context.getReference(eq(CompleteBean.class), levelOCaptor.capture())).thenReturn(
				entity);
		doCallRealMethod().when(em).getReference(CompleteBean.class, primaryKey, EACH_QUORUM);
		doCallRealMethod().when(em).getReference(eq(CompleteBean.class), eq(primaryKey),
				any(Optional.class));

		CompleteBean bean = em.getReference(CompleteBean.class, primaryKey, EACH_QUORUM);

		assertThat(bean).isSameAs(entity);
		assertThat(levelOCaptor.getValue().get()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_refresh() throws Exception
	{
		doCallRealMethod().when(em).refresh(entity);
		doCallRealMethod().when(em).refresh(entity, noConsistency);

		em.refresh(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(context).refresh(levelOCaptor.capture());
		assertThat(levelOCaptor.getValue().isPresent()).isFalse();
	}

	@Test
	public void should_refresh_with_consistency() throws Exception
	{
		doCallRealMethod().when(em).refresh(entity, EACH_QUORUM);
		doCallRealMethod().when(em).refresh(eq(entity), any(Optional.class));

		em.refresh(entity, EACH_QUORUM);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(context).refresh(levelOCaptor.capture());
		assertThat(levelOCaptor.getValue().get()).isSameAs(EACH_QUORUM);
	}

	@Test
	public void should_initialize_entity() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);

		doCallRealMethod().when(em).initialize(entity);
		em.initialize(entity);
		verify(proxifier).ensureProxy(entity);
		verify(initializer).initializeEntity(entity, entityMeta);
	}

	@Test
	public void should_initialize_entities() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);

		List<CompleteBean> entities = Arrays.asList(entity);
		doCallRealMethod().when(em).initialize(entities);
		em.initialize(entities);

		verify(proxifier).ensureProxy(entity);
		verify(initializer).initializeEntity(entity, entityMeta);
	}

	@Test
	public void should_unproxy_entity() throws Exception
	{
		when(proxifier.unproxy(entity)).thenReturn(entity);
		doCallRealMethod().when(em).unproxy(entity);
		CompleteBean actual = em.unproxy(entity);

		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_unproxy_collection_of_entity() throws Exception
	{
		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();

		when(proxifier.unproxy(proxies)).thenReturn(proxies);
		doCallRealMethod().when(em).unproxy(proxies);
		Collection<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unproxy_list_of_entity() throws Exception
	{
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();
		when(proxifier.unproxy(proxies)).thenReturn(proxies);

		doCallRealMethod().when(em).unproxy(proxies);
		List<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unproxy_set_of_entity() throws Exception
	{
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		when(proxifier.unproxy(proxies)).thenReturn(proxies);

		doCallRealMethod().when(em).unproxy(proxies);
		Set<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	private void forceMethodCallsOnMock()
	{
		doCallRealMethod().when(em).setInitializer(initializer);
		em.setInitializer(initializer);

		doCallRealMethod().when(em).setProxifier(proxifier);
		em.setProxifier(proxifier);

		doCallRealMethod().when(em).setEntityValidator(entityValidator);
		em.setEntityValidator(entityValidator);

		doCallRealMethod().when(em).setEntityMetaMap(entityMetaMap);
		em.setEntityMetaMap(entityMetaMap);
	}
}
