package info.archinnov.achilles.query;

import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.type.WideMap.BoundingMode;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.Statement;

/**
 * SliceQueryBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryBuilderTest
{

	@InjectMocks
	private SliceQueryBuilder builder;

	@Mock
	private SliceQueryValidator validator;

	@Mock
	private MethodInvoker invoker;

	@Mock
	private CQLStatementGenerator generator;

	@Mock
	private Statement statement;

	@Test
	public void should_generate_select_statement() throws Exception
	{
		List<String> compNames = Arrays.<String> asList();
		List<Method> compGetters = Arrays.<Method> asList();
		List<Object> startValues = Arrays.<Object> asList();
		List<Object> endValues = Arrays.<Object> asList();

		BoundingMode boundingMode = BoundingMode.INCLUSIVE_BOUNDS;

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.compNames(compNames)
				.compGetters(compGetters)
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);

		when(invoker.determineMultiKeyValues(10L, compGetters)).thenReturn(startValues);
		when(invoker.determineMultiKeyValues(11L, compGetters)).thenReturn(endValues);

		Statement actual = builder.generateSelectStatement(meta, 10L, 11L, boundingMode);

	}
}
