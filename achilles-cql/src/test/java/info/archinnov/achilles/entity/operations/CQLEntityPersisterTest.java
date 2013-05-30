package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesConfigurationContext;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.ImmutableMap;

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
	private AchillesMethodInvoker invoker;

	@Mock
	private Session session;

	@Mock
	private AchillesPersistenceContext context;

	@Mock
	private AchillesPersistenceContext joinContext;

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
		Whitebox.setInternalState(persister, "invoker", invoker);
		Whitebox.setInternalState(persister, "session", session);

		idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		when(context.getEntity()).thenReturn(entity);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getConfigContext()).thenReturn(configContext);
		when(entityMeta.getTableName()).thenReturn("table");
		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.getPropertyMetas()).thenReturn(metaMap);
		metaMap.clear();
	}

	@Test
	public void should_insert_only_primary_key_when_value_null() throws Exception
	{
		PropertyMeta<Void, String> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		metaMap.put("name", nameMeta);

		when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn(null);
		persister.persist(context);

		verify(session).execute(insertCaptor.capture());
		assertThat(insertCaptor.getValue().toString()).contains(
				"INSERT INTO table(id) VALUES (" + primaryKey + ")");
	}

	@Test
	public void should_insert_with_no_cascade() throws Exception
	{
		PropertyMeta<Void, String> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		metaMap.put("name", nameMeta);

		when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("my_name");
		persister.persist(context);

		verify(session).execute(insertCaptor.capture());

		assertThat(insertCaptor.getValue().toString()).contains(
				"INSERT INTO table(id,name) VALUES (" + primaryKey + ",'my_name')");
	}

	@Test
	public void should_cascade_with_join_simple() throws Exception
	{
		PropertyMeta<Void, String> joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.JOIN_SIMPLE)
				.joinMeta(joinMeta)
				.build();
		metaMap.put("name", joinSimpleMeta);

		when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn("my_name");
		when(context.newPersistenceContext(joinMeta, "my_name")).thenReturn(joinContext);
		doAnswer(new Answer<Void>()
		{

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable
			{
				statementsCaptor.getValue().add(new SimpleStatement("INSERT INTO test"));
				return null;
			}

		}).when(persister).cascadePersistOrEnsureExist(statementsCaptor.capture(), eq(joinContext));

		persister.persist(context);
		verify(persister).cascadePersistOrEnsureExist(any(List.class), eq(joinContext));
		verify(session).execute(batchCaptor.capture());

		assertThat(batchCaptor.getValue().toString()).contains(
				"BEGIN BATCH INSERT INTO test;INSERT INTO table(id,name) VALUES (" + primaryKey
						+ ",'my_name');APPLY BATCH;");
	}

	@Test
	public void should_cascade_with_join_collection() throws Exception
	{
		PropertyMeta<Void, String> joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.JOIN_LIST)
				.joinMeta(joinMeta)
				.build();
		metaMap.put("name", joinSimpleMeta);

		List<String> joinCollection = Arrays.asList("a", "b");
		when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(
				joinCollection);

		when(context.newPersistenceContext(eq(joinMeta), any(String.class)))
				.thenReturn(joinContext);

		doAnswer(new Answer<Void>()
		{

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable
			{
				statementsCaptor.getValue().add(new SimpleStatement("INSERT INTO test"));
				return null;
			}

		}).when(persister).cascadePersistOrEnsureExist(statementsCaptor.capture(), eq(joinContext));

		persister.persist(context);
		verify(persister, times(2)).cascadePersistOrEnsureExist(any(List.class), eq(joinContext));
		verify(session).execute(batchCaptor.capture());

		assertThat(batchCaptor.getValue().toString()).contains(
				"BEGIN BATCH INSERT INTO test;INSERT INTO test;INSERT INTO table(id,name) VALUES ("
						+ primaryKey + ",['a','b']);APPLY BATCH;");
	}

	@Test
	public void should_cascade_with_join_map() throws Exception
	{
		PropertyMeta<Void, String> joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.JOIN_MAP)
				.joinMeta(joinMeta)
				.build();
		metaMap.put("name", joinSimpleMeta);

		ImmutableMap<Integer, String> joinMap = ImmutableMap.of(1, "a", 2, "b");
		when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter())).thenReturn(joinMap);

		when(context.newPersistenceContext(eq(joinMeta), any(String.class)))
				.thenReturn(joinContext);

		doAnswer(new Answer<Void>()
		{

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable
			{
				statementsCaptor.getValue().add(new SimpleStatement("INSERT INTO test"));
				return null;
			}

		}).when(persister).cascadePersistOrEnsureExist(statementsCaptor.capture(), eq(joinContext));

		persister.persist(context);
		verify(persister, times(2)).cascadePersistOrEnsureExist(any(List.class), eq(joinContext));
		verify(session).execute(batchCaptor.capture());

		assertThat(batchCaptor.getValue().toString()).contains(
				"BEGIN BATCH INSERT INTO test;INSERT INTO test;INSERT INTO table(id,name) VALUES ("
						+ primaryKey + ",{1:'a',2:'b'});APPLY BATCH;");
	}

	@Test
	public void should_perform_cascade_persist() throws Exception
	{
		List<Statement> cascadePersist = new ArrayList<Statement>();
		when(configContext.isEnsureJoinConsistency()).thenReturn(false);
		persister.cascadePersistOrEnsureExist(cascadePersist, context);

	}
}
