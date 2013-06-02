package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesConfigurationContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLPersisterImpl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.persistence.CascadeType;

import mapping.entity.CompleteBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompleteBeanTestBuilder;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.Sets;

/**
 * CQLEntityPersisterTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityPersisterTest
{

	@Mock(answer = Answers.CALLS_REAL_METHODS)
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
	private AchillesConfigurationContext configContext;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private EntityMeta joinMeta;

	private PropertyMeta<Void, Long> idMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	private Map<String, PropertyMeta<?, ?>> metaMap = new HashMap<String, PropertyMeta<?, ?>>();

	@Captor
	private ArgumentCaptor<Insert> insertCaptor;

	@Captor
	private ArgumentCaptor<Batch> batchCaptor;

	@Captor
	private ArgumentCaptor<List<Statement>> statementsCaptor;

	@Before
	public void setUp() throws Exception
	{
		Whitebox.setInternalState(persister, "persisterImpl", persisterImpl);

		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);

		// when(context.getEntity()).thenReturn(entity);
		// when(context.getConfigContext()).thenReturn(configContext);
		// when(entityMeta.getTableName()).thenReturn("table");
		// when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
		// when(entityMeta.getPropertyMetas()).thenReturn(metaMap);
		// metaMap.clear();
	}

	@Test
	public void should_persist() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(false);

		persister.persist(context);

		verify(persisterImpl).persist(persister, context);
	}

	@Test
	public void should_not_persist_if_widerow() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(true);

		persister.persist(context);

		verifyZeroInteractions(persisterImpl);
	}

	@Test
	public void should_cascade_persist_on_cascade_ALL() throws Exception
	{
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setCascadeTypes(Sets.newHashSet(CascadeType.ALL));

		when(entityMeta.isWideRow()).thenReturn(false);

		Object actual = persister.cascadePersistOrEnsureExist(context, joinProperties);

		assertThat(actual).isSameAs(primaryKey);
		verify(persisterImpl).persist(persister, context);
	}

	@Test
	public void should_cascade_persist_on_cascade_PERSIST() throws Exception
	{
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setCascadeTypes(Sets.newHashSet(CascadeType.PERSIST));

		when(entityMeta.isWideRow()).thenReturn(false);

		Object actual = persister.cascadePersistOrEnsureExist(context, joinProperties);

		assertThat(actual).isSameAs(primaryKey);
		verify(persisterImpl).persist(persister, context);
	}

	@Test
	public void should_ensure_entity_exist() throws Exception
	{
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setCascadeTypes(new HashSet<CascadeType>());
		joinProperties.setEntityMeta(entityMeta);

		when(context.getConfigContext().isEnsureJoinConsistency()).thenReturn(true);

		when(persisterImpl.doesEntityExist(context)).thenReturn(true);

		Object actual = persister.cascadePersistOrEnsureExist(context, joinProperties);

		assertThat(actual).isSameAs(primaryKey);
	}

	@Test
	public void should_not_check_for_entity_existence_on_cascade() throws Exception
	{
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setCascadeTypes(new HashSet<CascadeType>());
		joinProperties.setEntityMeta(entityMeta);

		when(context.getConfigContext().isEnsureJoinConsistency()).thenReturn(false);

		Object actual = persister.cascadePersistOrEnsureExist(context, joinProperties);
		assertThat(actual).isSameAs(primaryKey);

		verifyZeroInteractions(persisterImpl);
	}

	@Test
	public void should_remove() throws Exception
	{
		persister.remove(context);

		verify(persisterImpl).remove(context);
	}
}
