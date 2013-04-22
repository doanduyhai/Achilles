package integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;

import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Cql3Test
 * 
 * @author DuyHai DOAN
 * 
 */
@Ignore
public class Cql3Test
{
	private Session session = CQLCassandraDaoTest.getCqlSession();

	@Before
	public void setUp()
	{
		String query = "create table cql3_user(id bigint,firstname text,lastname text, age int, primary key(id))";
		session.execute(query);
	}

	@Test
	public void should_insert() throws Exception
	{

		long userId = RandomUtils.nextLong();
		String insert = QueryBuilder.insertInto("cql3_user") //
				.value("id", userId) //
				.value("firstname", "FN") //
				.value("lastname", "LN") //
				.value("age", 35)//
				.toString();

		session.execute(insert);

		Clause filterById = QueryBuilder.eq("id", userId);
		String select = QueryBuilder.select("firstname", "age").from("cql3_user").where(filterById)
				.toString();

		Row row = session.execute(select).all().get(0);

		assertThat(row.getString("firstname")).isEqualTo("FN");
		assertThat(row.getInt("age")).isEqualTo(35);
	}

	@Test
	public void should_batch_insert_with_different_consistency_levels() throws Exception
	{
		String insert1 = QueryBuilder.insertInto("cql3_user") //
				.value("id", 100001L) //
				.value("firstname", "FN1") //
				.value("lastname", "LN1") //
				.value("age", 31) //
				.toString();

		String insert2 = QueryBuilder.insertInto("cql3_user") //
				.value("id", 100002L) //
				.value("firstname", "FN2") //
				.value("lastname", "LN2") //
				.value("age", 32)//
				.toString();

		String insert3 = QueryBuilder.insertInto("cql3_user") //
				.value("id", 100003L) //
				.value("firstname", "FN3") //
				.value("lastname", "LN3") //
				.value("age", 33)//
				.toString();

		Query batch = QueryBuilder //
				.batch(new SimpleStatement(insert1)) //
				.add(new SimpleStatement(insert2))//
				.add(new SimpleStatement(insert3))//
				.setConsistencyLevel(ConsistencyLevel.ALL)//
				.enableTracing();

		session.execute(batch);

	}

	@Test
	public void should_execute_prepared_statement() throws Exception
	{

		PreparedStatement preparedStatement = session
				.prepare("INSERT INTO cql3_user(id,firstname,lastname,age) VALUES (?,?,?,?)");
		BoundStatement boundStatement = preparedStatement.bind(100004L, "FN4", "LN4", new Integer(
				34));
		boundStatement.setConsistencyLevel(ConsistencyLevel.ANY);

		ResultSet result = session.execute(boundStatement);

		System.out.println(result.getExecutionInfo().getQueriedHost());
	}

	@After
	public void cleanUp()
	{
		String listAllTables = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='achilles'";
		List<Row> rows = session.execute(listAllTables).all();

		for (Row row : rows)
		{
			session.execute(new SimpleStatement("truncate " + row.getString("columnfamily_name")));
		}
	}
}
