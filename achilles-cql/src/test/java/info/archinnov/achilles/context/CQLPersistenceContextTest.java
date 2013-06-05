package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

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
import com.datastax.driver.core.Session;

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
	private AchillesConfigurationContext configurationContext;

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
	public void should_push_bound_statement() throws Exception
	{
		BoundStatement bs = mock(BoundStatement.class);

		context.pushBoundStatement(bs, EACH_QUORUM);

		verify(flushContext).pushBoundStatement(bs, EACH_QUORUM);
	}

	@Test
	public void should_execute_immediate_with_consistency() throws Exception
	{
		Session session = mock(Session.class);
		BoundStatement bs = mock(BoundStatement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(flushContext.executeImmediateWithConsistency(session, bs, meta)).thenReturn(resultSet);

		ResultSet actual = context.executeImmediateWithConsistency(session, bs);

		assertThat(actual).isSameAs(resultSet);
	}
}
