package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.statement.CQLPreparedStatementGenerator;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.HashMap;
import java.util.Map;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.Cache;

/**
 * CQLDaoContextBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLDaoContextBuilderTest
{
	@InjectMocks
	private CQLDaoContextBuilder builder;

	@Mock
	private Session session;

	@Mock
	private CQLPreparedStatementGenerator queryGenerator;

	@Mock
	private PreparedStatement insertPS;

	@Mock
	private PreparedStatement selectForExistenceCheckPS;

	@Mock
	private PreparedStatement selectEagerPS;

	@Mock
	private Map<String, PreparedStatement> removePSs;

	@Mock
	private Map<CQLQueryType, PreparedStatement> counterQueryMap;

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(builder, "queryGenerator", queryGenerator);
		Whitebox.setInternalState(builder, "session", session);
	}

	@Test
	public void should_build_dao_context() throws Exception
	{
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		EntityMeta meta = new EntityMeta();
		entityMetaMap.put(CompleteBean.class, meta);

		when(queryGenerator.prepareInsertPS(session, meta)).thenReturn(insertPS);
		when(queryGenerator.prepareSelectEagerPS(session, meta)).thenReturn(selectEagerPS);
		when(queryGenerator.prepareRemovePSs(session, meta)).thenReturn(removePSs);
		when(queryGenerator.prepareSimpleCounterQueryMap(session)).thenReturn(counterQueryMap);

		CQLDaoContext actual = builder.build(entityMetaMap);

		assertThat(
				(Map<Class<?>, PreparedStatement>) Whitebox.getInternalState(actual, "insertPSs"))
				.containsValue(insertPS);
		assertThat(
				(Map<Class<?>, PreparedStatement>) Whitebox.getInternalState(actual,
						"selectEagerPSs")).containsValue(selectEagerPS);
		assertThat(
				(Map<Class<?>, Map<String, PreparedStatement>>) Whitebox.getInternalState(actual,
						"removePSs")).containsKey(CompleteBean.class);

		assertThat(
				(Cache<StatementCacheKey, PreparedStatement>) Whitebox.getInternalState(actual,
						"dynamicPSCache")).isInstanceOf(Cache.class);

		assertThat(
				(Map<CQLQueryType, PreparedStatement>) Whitebox.getInternalState(actual,
						"counterQueryMap")).isSameAs(counterQueryMap);
	}
}
