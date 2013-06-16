package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;

import java.util.Arrays;
import java.util.List;

import mapping.entity.CompleteBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * CQLPersistenceContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceContextTest
{
	@InjectMocks
	private CQLPersistenceContext context;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private CQLAbstractFlushContext flushContext;

	@Mock
	private ConfigurationContext configurationContext;

	@Mock
	private CQLEntityLoader loader;

	@Mock
	private CQLEntityPersister persister;

	@Mock
	private CQLEntityMerger merger;

	@Mock
	private CQLEntityProxifier proxifier;

	@Mock
	private EntityRefresher<CQLPersistenceContext> refresher;

	private EntityMeta meta;

	private PropertyMeta<?, ?> idMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	@Before
	public void setUp() throws Exception
	{
		idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.accessors()
				.build();

		meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setEntityClass(CompleteBean.class);

		Whitebox.setInternalState(context, "entityMeta", meta);
		Whitebox.setInternalState(context, "loader", loader);
		Whitebox.setInternalState(context, "merger", merger);
		Whitebox.setInternalState(context, "persister", persister);
		Whitebox.setInternalState(context, "refresher", refresher);
		Whitebox.setInternalState(context, "proxifier", proxifier);
	}

	@Test
	public void should_create_new_persistence_context_for_join_entity() throws Exception
	{

		CQLPersistenceContext joinContext = context.newPersistenceContext(meta, entity);

		assertThat(joinContext.getEntity()).isSameAs(entity);
		assertThat((Class) joinContext.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(joinContext.getEntityMeta()).isSameAs(meta);
		assertThat(joinContext.getPrimaryKey()).isEqualTo(primaryKey);
	}

	@Test
	public void should_create_new_persistence_context_with_id_and_type() throws Exception
	{
		CQLPersistenceContext joinContext = context.newPersistenceContext(CompleteBean.class, meta,
				primaryKey);

		assertThat((Class) joinContext.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(joinContext.getEntityMeta()).isSameAs(meta);
		assertThat(joinContext.getPrimaryKey()).isEqualTo(primaryKey);

	}

	@Test
	public void should_check_for_entity_existence() throws Exception
	{
		when(daoContext.checkForEntityExistence(context)).thenReturn(true);

		assertThat(context.checkForEntityExistence()).isTrue();
	}

	@Test
	public void should_eager_load_entity() throws Exception
	{
		Row row = mock(Row.class);
		when(daoContext.eagerLoadEntity(context)).thenReturn(row);

		assertThat(context.eagerLoadEntity()).isSameAs(row);
	}

	@Test
	public void should_load_property() throws Exception
	{
		Row row = mock(Row.class);
		when(daoContext.loadProperty(context, idMeta)).thenReturn(row);

		assertThat(context.loadProperty(idMeta)).isSameAs(row);
	}

	@Test
	public void should_bind_for_insert() throws Exception
	{
		context.bindForInsert();

		verify(daoContext).bindForInsert(context);
	}

	@Test
	public void should_bind_for_update() throws Exception
	{
		List<PropertyMeta<?, ?>> pms = Arrays.asList();
		context.bindForUpdate(pms);

		verify(daoContext).bindForUpdate(context, pms);
	}

	@Test
	public void should_bind_for_removal() throws Exception
	{
		context.bindForRemoval("table", EACH_QUORUM);

		verify(daoContext).bindForRemoval(context, "table", EACH_QUORUM);
	}

	@Test
	public void should_bind_for_simple_counter_removal() throws Exception
	{
		PropertyMeta<Void, Long> counterMeta = new PropertyMeta<Void, Long>();
		EntityMeta meta = new EntityMeta();

		context.bindForSimpleCounterRemoval(meta, counterMeta, 11L);

		verify(daoContext).bindForSimpleCounterDelete(context, meta, counterMeta, 11L);
	}

	@Test
	public void should_push_bound_statement() throws Exception
	{
		BoundStatement bs = mock(BoundStatement.class);

		context.pushBoundStatement(bs, EACH_QUORUM);

		verify(flushContext).pushBoundStatement(bs, EACH_QUORUM);
	}

	@Test
	public void should_execute_immediate_with_consistency() throws Exception
	{
		BoundStatement bs = mock(BoundStatement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(flushContext.executeImmediateWithConsistency(bs, EACH_QUORUM)).thenReturn(resultSet);

		ResultSet actual = context.executeImmediateWithConsistency(bs, EACH_QUORUM);

		assertThat(actual).isSameAs(resultSet);
	}

	@Test
	public void should_persist() throws Exception
	{
		context.persist();
		verify(persister).persist(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_merge() throws Exception
	{
		when(merger.merge(context, entity)).thenReturn(entity);

		CompleteBean merged = context.merge(entity);

		assertThat(merged).isSameAs(entity);
		verify(flushContext).flush();
	}

	@Test
	public void should_remove() throws Exception
	{
		context.remove();
		verify(persister).remove(context);
		verify(flushContext).flush();
	}

	@Test
	public void should_find() throws Exception
	{
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean found = context.find(CompleteBean.class);

		assertThat(context.isLoadEagerFields()).isTrue();
		assertThat(found).isSameAs(entity);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(loader.load(context, CompleteBean.class)).thenReturn(entity);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean found = context.getReference(CompleteBean.class);

		assertThat(context.isLoadEagerFields()).isFalse();
		assertThat(found).isSameAs(entity);
	}

	@Test
	public void should_refresh() throws Exception
	{
		context.refresh();
		verify(refresher).refresh(context);
	}
}
