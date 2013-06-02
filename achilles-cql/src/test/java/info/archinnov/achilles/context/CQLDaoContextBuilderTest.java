package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.helper.CQLPreparedStatementHelper;

import java.util.HashMap;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

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
	private CQLPreparedStatementHelper helper;

	@Mock
	private PreparedStatement insertPS;

	@Mock
	private PreparedStatement selectForExistenceCheckPS;

	@Mock
	private PreparedStatement selectEagerPS;

	@Mock
	private Map<String, PreparedStatement> removePSs;

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(builder, "preparedStatementHelper", helper);
	}

	@Test
	public void should_build_dao_context() throws Exception
	{
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		EntityMeta meta = new EntityMeta();
		entityMetaMap.put(CompleteBean.class, meta);

		when(helper.prepareInsertPS(session, meta)).thenReturn(insertPS);
		when(helper.prepareSelectForExistenceCheckPS(session, meta)).thenReturn(
				selectForExistenceCheckPS);
		when(helper.prepareSelectEagerPS(session, meta)).thenReturn(selectEagerPS);
		when(helper.prepareRemovePSs(session, meta)).thenReturn(removePSs);

		CQLDaoContext actual = builder.build(session, entityMetaMap);

		assertThat(actual.getInsertPSs()).containsValue(insertPS);
		assertThat(actual.getSelectForExistenceCheckPSs()).containsValue(selectForExistenceCheckPS);
		assertThat(actual.getSelectEagerPSs()).containsValue(selectEagerPS);
		assertThat(actual.getRemovePSs()).containsValue(removePSs);
	}
}
