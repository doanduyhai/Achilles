package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.CQLPreparedStatementBinder;

import java.util.HashMap;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompleteBeanTestBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

/**
 * CQLDaoContextTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CQLDaoContextTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private CQLDaoContext daoContext;

	private Map<Class<?>, PreparedStatement> insertPSs = new HashMap<Class<?>, PreparedStatement>();

	private Map<Class<?>, PreparedStatement> selectForExistenceCheckPSs = new HashMap<Class<?>, PreparedStatement>();

	private Map<Class<?>, PreparedStatement> selectEagerPSs = new HashMap<Class<?>, PreparedStatement>();

	private Map<Class<?>, Map<String, PreparedStatement>> removePSs = new HashMap<Class<?>, Map<String, PreparedStatement>>();

	@Mock
	private Session session;

	@Mock
	private CQLPreparedStatementBinder binder;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private PreparedStatement ps;

	@Mock
	private BoundStatement bs;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(daoContext, "binder", binder);
		Whitebox.setInternalState(daoContext, "insertPSs", insertPSs);
		Whitebox.setInternalState(daoContext, "selectForExistenceCheckPSs",
				selectForExistenceCheckPSs);
		Whitebox.setInternalState(daoContext, "selectEagerPSs", selectEagerPSs);
		Whitebox.setInternalState(daoContext, "removePSs", removePSs);

		when(context.getEntityMeta()).thenReturn(entityMeta);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntity()).thenReturn(entity);

		insertPSs.clear();
		selectEagerPSs.clear();
		selectForExistenceCheckPSs.clear();
		removePSs.clear();
	}

	@Test
	public void should_bind_for_insert() throws Exception
	{
		insertPSs.put(CompleteBean.class, ps);
		when(binder.bind(ps, entityMeta, entity)).thenReturn(bs);

		BoundStatement actual = daoContext.bindForInsert(context);

		assertThat(actual).isSameAs(bs);
	}

	@Test
	public void should_check_for_entity_existence() throws Exception
	{
		selectForExistenceCheckPSs.put(CompleteBean.class, ps);
		when(binder.bind(ps, entityMeta, entity)).thenReturn(bs);

		ResultSet resultSet = mock(ResultSet.class, RETURNS_DEEP_STUBS);
		when(session.execute(bs)).thenReturn(resultSet);
		when(resultSet.all().size()).thenReturn(1);

		boolean actual = daoContext.checkForEntityExistence(context);

		assertThat(actual).isTrue();
	}

	@Test
	public void should_bind_for_removal() throws Exception
	{
		removePSs.put(CompleteBean.class, ImmutableMap.of("table", ps));
		when(binder.bind(ps, entityMeta, entity)).thenReturn(bs);

		BoundStatement actual = daoContext.bindForRemove(context, "table");

		assertThat(actual).isSameAs(bs);
	}

	@Test
	public void should_exception_when_removal_ps_not_found_for_a_table() throws Exception
	{
		removePSs.put(CompleteBean.class, ImmutableMap.of("some_table", ps));

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot find prepared statement for deletion for table 'table'");

		daoContext.bindForRemove(context, "table");
	}

}
