package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.AchillesEntityInitializer;
import info.archinnov.achilles.entity.operations.AchillesEntityLoader;
import info.archinnov.achilles.entity.operations.AchillesEntityMerger;
import info.archinnov.achilles.entity.operations.AchillesEntityPersister;
import info.archinnov.achilles.entity.operations.AchillesEntityProxifier;
import info.archinnov.achilles.entity.operations.AchillesEntityRefresher;
import info.archinnov.achilles.entity.operations.AchillesEntityValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;

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
	private AchillesEntityManager em;

	@Mock
	private AchillesEntityPersister persister;

	@Mock
	private AchillesEntityLoader loader;

	@Mock
	private AchillesEntityMerger merger;

	@Mock
	private AchillesEntityRefresher refresher;

	@Mock
	private AchillesEntityInitializer initializer;

	@Mock
	private AchillesEntityProxifier proxifier;

	@Mock
	private AchillesEntityValidator entityValidator;

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

	@Mock
	private EntityMeta entityMeta;

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

		em.persist(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);

		verify(persister).persist(context);
		verify(context).flush();

	}

	@Test
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(true);
		doCallRealMethod().when(em).persist(entity);

		exception.expect(IllegalStateException.class);

		em.persist(entity);
	}

	@Test
	public void should_merge() throws Exception
	{

		when(merger.mergeEntity(context, entity)).thenReturn(entity);
		doCallRealMethod().when(em).merge(entity);

		CompleteBean mergedEntity = em.merge(entity);
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);

		verify(context).flush();

		assertThat(mergedEntity).isSameAs(entity);
	}

	@Test
	public void should_remove() throws Exception
	{
		doCallRealMethod().when(em).remove(entity);
		em.remove(entity);
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(persister).remove(context);
		verify(context).flush();
	}

	@Test
	public void should_find() throws Exception
	{
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		doCallRealMethod().when(em).find(CompleteBean.class, primaryKey);
		CompleteBean bean = em.find(CompleteBean.class, primaryKey);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_return_null_when_not_found() throws Exception
	{
		when(loader.load(context, CompleteBean.class)).thenReturn(null);
		doCallRealMethod().when(em).find(CompleteBean.class, primaryKey);
		CompleteBean bean = em.find(CompleteBean.class, primaryKey);

		assertThat(bean).isNull();
		verifyZeroInteractions(proxifier);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		doCallRealMethod().when(em).getReference(CompleteBean.class, primaryKey);
		CompleteBean bean = em.getReference(CompleteBean.class, primaryKey);

		assertThat(bean).isSameAs(entity);
		verify(context).setLoadEagerFields(false);
	}

	@Test
	public void should_get_flush_mode() throws Exception
	{
		doCallRealMethod().when(em).getFlushMode();
		FlushModeType flushMode = em.getFlushMode();

		assertThat(flushMode).isEqualTo(FlushModeType.AUTO);
	}

	@Test
	public void should_refresh() throws Exception
	{

		doCallRealMethod().when(em).refresh(entity);
		em.refresh(entity);

		verify(proxifier).ensureProxy(entity);
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotWideRow(entity, entityMetaMap);
		verify(refresher).refresh(context);

	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_set_flush_mode() throws Exception
	{
		doCallRealMethod().when(em).setFlushMode(FlushModeType.COMMIT);
		em.setFlushMode(FlushModeType.COMMIT);
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
		doCallRealMethod().when(em).setPersister(persister);
		em.setPersister(persister);

		doCallRealMethod().when(em).setLoader(loader);
		em.setLoader(loader);

		doCallRealMethod().when(em).setMerger(merger);
		em.setMerger(merger);

		doCallRealMethod().when(em).setRefresher(refresher);
		em.setRefresher(refresher);

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
