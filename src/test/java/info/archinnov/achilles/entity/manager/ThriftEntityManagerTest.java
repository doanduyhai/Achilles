package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.builder.EntityMetaTestBuilder;
import info.archinnov.achilles.entity.operations.EntityBatcher;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.EntityValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;

import mapping.entity.CompleteBean;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftEntityManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
	"unchecked"
})
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityManager em;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityMerger merger;

	@Mock
	private EntityRefresher refresher;

	@Mock
	private EntityBatcher batcher;

	@Mock
	private EntityInitializer initializer;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private EntityValidator entityValidator;

	private EntityMeta<Long> entityMeta;

	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private GenericDynamicCompositeDao<Long> entityDao;

	@Mock
	private GenericDynamicCompositeDao<Long> joinEntityDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Long> joinMutator;

	@Captor
	ArgumentCaptor<Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>>> mutatorMapCaptor;

	@Captor
	ArgumentCaptor<PersistenceContext<Long>> contextCaptor;

	private Long primaryKey = 1165446L;
	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).name("name")
			.buid();

	@Before
	public void setUp() throws Exception
	{
		Whitebox.setInternalState(em, "persister", persister);
		merger.setPersister(persister);
		Whitebox.setInternalState(em, "loader", loader);
		Whitebox.setInternalState(em, "merger", merger);
		Whitebox.setInternalState(em, "refresher", refresher);
		Whitebox.setInternalState(em, "batcher", batcher);
		Whitebox.setInternalState(em, "initializer", initializer);
		Whitebox.setInternalState(em, "proxifier", proxifier);
		Whitebox.setInternalState(em, "entityValidator", entityValidator);
		Whitebox.setInternalState(em, "entityMetaMap", entityMetaMap);

		idMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, Long.class) //
				.field("id") //
				.accesors() //
				.type(SIMPLE) //
				.build();
		entityMeta = EntityMetaTestBuilder.builder(idMeta).build();

		when((Class<CompleteBean>) proxifier.deriveBaseClass(entity))
				.thenReturn(CompleteBean.class);
		when((EntityMeta<Long>) entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);

	}

	@Test
	public void should_persist() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(false);

		em.persist(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotCFDirectMapping(entity, entityMetaMap);

		verify(persister).persist(contextCaptor.capture());

		assertThat(contextCaptor.getValue().getEntity()).isEqualTo(entity);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception
	{
		when(proxifier.isProxy(entity)).thenReturn(true);

		em.persist(entity);
	}

	@Test
	public void should_merge() throws Exception
	{

		when(merger.mergeEntity(contextCaptor.capture())).thenReturn(entity);

		CompleteBean mergedEntity = em.merge(entity);
		verify(entityValidator).validateEntity(entity, entityMetaMap);

		assertThat(contextCaptor.getValue().getEntity()).isEqualTo(entity);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

		assertThat(mergedEntity).isSameAs(entity);
	}

	@Test
	public void should_remove() throws Exception
	{

		doNothing().when(persister).remove(contextCaptor.capture());

		em.remove(entity);
		verify(proxifier).ensureProxy(entity);
		verify(entityValidator).validateEntity(entity, entityMetaMap);
		assertThat(contextCaptor.getValue().getEntity()).isEqualTo(entity);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test
	public void should_find() throws Exception
	{
		when(loader.load(contextCaptor.capture())).thenReturn(entity);
		when(proxifier.buildProxy(eq(entity), contextCaptor.capture())).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, primaryKey);

		assertThat(bean).isSameAs(entity);
		assertThat(contextCaptor.getValue().getPrimaryKey()).isEqualTo(primaryKey);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(loader.load(contextCaptor.capture())).thenReturn(entity);
		when(proxifier.buildProxy(eq(entity), contextCaptor.capture())).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, primaryKey);

		assertThat(bean).isSameAs(entity);
		assertThat(contextCaptor.getValue().getPrimaryKey()).isEqualTo(primaryKey);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test
	public void should_get_flush_mode() throws Exception
	{
		FlushModeType flushMode = em.getFlushMode();

		assertThat(flushMode).isEqualTo(FlushModeType.AUTO);
	}

	@Test
	public void should_refresh() throws Exception
	{
		doNothing().when(refresher).refresh(contextCaptor.capture());

		em.refresh(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(entityValidator).validateNotCFDirectMapping(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);

		assertThat(contextCaptor.getValue().getEntity()).isEqualTo(entity);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_set_flush_mode() throws Exception
	{
		em.setFlushMode(FlushModeType.COMMIT);
	}

	@Test
	public void should_start_batch() throws Exception
	{

		em.startBatch(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);

		verify(batcher).startBatchForEntity(eq(entity), contextCaptor.capture());

		assertThat(contextCaptor.getValue().getEntity()).isEqualTo(entity);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test
	public void should_exception_when_trying_to_batch_transient_entity() throws Exception
	{
		CompleteBean completeBean = new CompleteBean();

		doThrow(new IllegalStateException("test")).when(proxifier).ensureProxy(completeBean);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("test");

		em.startBatch(completeBean);
	}

	@Test
	public void should_end_batch() throws Exception
	{

		em.endBatch(entity);

		verify(entityValidator).validateEntity(entity, entityMetaMap);
		verify(proxifier).ensureProxy(entity);
		verify(batcher).endBatch(eq(entity), contextCaptor.capture());
		assertThat(contextCaptor.getValue().getEntity()).isEqualTo(entity);
		assertThat(contextCaptor.getValue().getEntityMeta()).isEqualTo(entityMeta);

	}

	@Test
	public void should_initialize_entity() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		em.initialize(entity);
		verify(proxifier).ensureProxy(entity);
		verify(initializer).initializeEntity(entity, entityMeta);
	}

	@Test
	public void should_initialize_entities() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		em.initialize(Arrays.asList(entity));
		verify(proxifier).ensureProxy(entity);
		verify(initializer).initializeEntity(entity, entityMeta);
	}

	@Test
	public void should_unproxy_entity() throws Exception
	{
		when(proxifier.unproxy(entity)).thenReturn(entity);

		CompleteBean actual = em.unproxy(entity);

		assertThat(actual).isSameAs(entity);
	}

	@Test
	public void should_unproxy_collection_of_entity() throws Exception
	{
		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();

		when(proxifier.unproxy(proxies)).thenReturn(proxies);

		Collection<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unproxy_list_of_entity() throws Exception
	{
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();

		when(proxifier.unproxy(proxies)).thenReturn(proxies);

		List<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

	@Test
	public void should_unproxy_set_of_entity() throws Exception
	{
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		when(proxifier.unproxy(proxies)).thenReturn(proxies);

		Set<CompleteBean> actual = em.unproxy(proxies);

		assertThat(actual).isSameAs(proxies);
	}

}
