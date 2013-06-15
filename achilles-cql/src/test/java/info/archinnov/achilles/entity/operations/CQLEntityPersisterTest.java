package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLPersisterImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import mapping.entity.CompleteBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;

/**
 * CQLEntityPersisterTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityPersisterTest
{
	@InjectMocks
	private CQLEntityPersister persister;

	@Mock
	private CQLPersisterImpl persisterImpl;

	@Mock
	private Session session;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private CQLPersistenceContext context;

	@Mock
	private CQLPersistenceContext joinContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private EntityMeta joinMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	private List<PropertyMeta<?, ?>> allMetas = new ArrayList<PropertyMeta<?, ?>>();
	@Captor
	private ArgumentCaptor<Set<PropertyMeta<?, ?>>> metaSetCaptor;

	@Captor
	private ArgumentCaptor<Insert> insertCaptor;

	@Captor
	private ArgumentCaptor<Batch> batchCaptor;

	@Captor
	private ArgumentCaptor<List<Statement>> statementsCaptor;

	@Before
	public void setUp() throws Exception
	{
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(context.getEntity()).thenReturn(entity);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(entityMeta.getAllMetas()).thenReturn(allMetas);
	}

	@Test
	public void should_persist() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(false);
		when(context.addToProcessingList(entity)).thenReturn(true);
		persister.persist(context);

		verify(persisterImpl).persist(context);
		verify(persisterImpl).cascadePersist(eq(persister), eq(context), metaSetCaptor.capture());

		assertThat(metaSetCaptor.getValue()).isEmpty();
	}

	@Test
	public void should_not_persist_twice_the_same_entity() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(false);
		when(context.addToProcessingList(entity)).thenReturn(true, false);
		persister.persist(context);
		persister.persist(context);

		verify(persisterImpl, times(1)).persist(context);
		verify(persisterImpl, times(1)).cascadePersist(eq(persister), eq(context),
				metaSetCaptor.capture());

		assertThat(metaSetCaptor.getValue()).isEmpty();
	}

	@Test
	public void should_not_persist_if_widerow() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(true);

		persister.persist(context);

		verifyZeroInteractions(persisterImpl);
	}

	@Test
	public void should_ensure_entity_exist() throws Exception
	{

		when(context.getConfigContext().isEnsureJoinConsistency()).thenReturn(true);
		when(context.addToProcessingList(entity)).thenReturn(true);

		persister.persist(context);
		verify(persisterImpl).persist(context);
		verify(persisterImpl).ensureEntitiesExist(eq(context), metaSetCaptor.capture());

		assertThat(metaSetCaptor.getValue()).isEmpty();
	}

	@Test
	public void should_remove() throws Exception
	{
		persister.remove(context);

		verify(persisterImpl).remove(context);
	}
}
