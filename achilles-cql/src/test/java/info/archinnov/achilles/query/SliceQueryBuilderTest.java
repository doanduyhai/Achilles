package info.archinnov.achilles.query;

import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.statement.CQLStatementGenerator;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;

/**
 * SliceQueryBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class SliceQueryBuilderTest
{

	@InjectMocks
	private SliceQueryBuilder builder;

	@Mock
	private SliceQueryValidator validator;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLStatementGenerator generator;

	@Mock
	private Select select;

	@Mock
	private Statement statement;

	@Test
	public void should_generate_select_statement() throws Exception
	{}

}
