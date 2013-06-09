package integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTables;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Cql3Test
 * 
 * @author DuyHai DOAN
 * 
 */
public class Cql3Test
{

	private Session session = CQLCassandraDaoTest.getCqlSession();

	@Test
	public void should_insert_list() throws Exception
	{
		long userId = RandomUtils.nextLong();
		String insertList = QueryBuilder.insertInto("cql3_list") //
				.value("id", userId)
				.value("myList", Arrays.asList("a", "b", "c"))
				.toString();
		session.execute(insertList);

		insertList = QueryBuilder.insertInto("cql3_list") //
				.value("id", userId)
				.value("myList", Arrays.asList("a", "b"))
				.toString();
		session.execute(insertList);

		String select = QueryBuilder
				.select("myList")
				.from("cql3_list")
				.where(QueryBuilder.eq("id", userId))
				.toString();

		Row row = session.execute(select).one();

		List<String> myList = row.getList("myList", String.class);
		assertThat(myList).hasSize(2);
		assertThat(myList).containsExactly("a", "b");
	}

	@Test
	public void should_update_list() throws Exception
	{
		long userId = RandomUtils.nextLong();
		Update updateList = QueryBuilder.update("cql3_list"); //
		updateList.where(QueryBuilder.eq("id", userId));
		updateList.with(QueryBuilder.set("myList", Arrays.asList("a", "b", "c")));

		session.execute(updateList);

		updateList = QueryBuilder.update("cql3_list"); //
		updateList.where(QueryBuilder.eq("id", userId));
		updateList.with(QueryBuilder.set("myList", Arrays.asList("a", "b")));

		session.execute(updateList);

		String select = QueryBuilder
				.select("myList")
				.from("cql3_list")
				.where(QueryBuilder.eq("id", userId))
				.toString();

		Row row = session.execute(select).one();

		List<String> myList = row.getList("myList", String.class);
		assertThat(myList).hasSize(2);
		assertThat(myList).containsExactly("a", "b");
	}

	@Test
	public void should_manipulate_list() throws Exception
	{
		long userId = RandomUtils.nextLong();
		String insertList = QueryBuilder.insertInto("cql3_list") //
				.value("id", userId)
				.value("myList", Arrays.asList("a", "b", "c"))
				.toString();
		session.execute(insertList);

		Update updateList = QueryBuilder.update("cql3_list"); //
		updateList.where(QueryBuilder.eq("id", userId));
		updateList.with(QueryBuilder.setIdx("myList", 1, "b_bis"));
		session.execute(updateList);

		updateList = QueryBuilder.update("cql3_list"); //
		updateList.where(QueryBuilder.eq("id", userId));
		updateList.with(QueryBuilder.prepend("myList", "alpha"));
		session.execute(updateList);

		updateList = QueryBuilder.update("cql3_list"); //
		updateList.where(QueryBuilder.eq("id", userId));
		updateList.with(QueryBuilder.append("myList", "zeta"));
		session.execute(updateList);

		String select = QueryBuilder
				.select("myList")
				.from("cql3_list")
				.where(QueryBuilder.eq("id", userId))
				.toString();

		Row row = session.execute(select).one();

		List<String> myList = row.getList("myList", String.class);
		assertThat(myList).hasSize(5);
		assertThat(myList).containsExactly("alpha", "a", "b_bis", "c", "zeta");
	}

	@Test
	public void should_insert_set() throws Exception
	{
		long userId = RandomUtils.nextLong();
		String insertSet = QueryBuilder.insertInto("cql3_set") //
				.value("id", userId)
				.value("mySet", Sets.newHashSet("a", "b", "c"))
				.toString();
		session.execute(insertSet);

		insertSet = QueryBuilder.insertInto("cql3_set") //
				.value("id", userId)
				.value("mySet", Sets.newHashSet("a", "c"))
				.toString();
		session.execute(insertSet);

		String select = QueryBuilder
				.select("mySet")
				.from("cql3_set")
				.where(QueryBuilder.eq("id", userId))
				.toString();

		Row row = session.execute(select).one();

		Set<String> myList = row.getSet("mySet", String.class);
		assertThat(myList).hasSize(2);
		assertThat(myList).contains("a", "c");
	}

	@Test
	public void should_insert_map() throws Exception
	{
		long userId = RandomUtils.nextLong();
		String insertMap = QueryBuilder.insertInto("cql3_map") //
				.value("id", userId)
				.value("myMap", ImmutableMap.of(1, "a", 2, "b", 3, "c"))
				.toString();
		session.execute(insertMap);

		insertMap = QueryBuilder.insertInto("cql3_map") //
				.value("id", userId)
				.value("myMap", ImmutableMap.of(1, "a", 2, "b"))
				.toString();
		session.execute(insertMap);

		String select = QueryBuilder
				.select("myMap")
				.from("cql3_map")
				.where(QueryBuilder.eq("id", userId))
				.toString();

		Row row = session.execute(select).one();

		Map<Integer, String> myMap = row.getMap("myMap", Integer.class, String.class);
		assertThat(myMap).hasSize(2);
		assertThat(myMap).containsKey(1);
		assertThat(myMap).containsKey(2);

		assertThat(myMap).containsValue("a");
		assertThat(myMap).containsValue("b");
	}

	@Test
	public void should_insert() throws Exception
	{

		long userId = RandomUtils.nextLong();
		String insert = QueryBuilder.insertInto("cql3_user") //
				.value("id", userId)
				.value("firstname", "FN")
				// .value("lastname", null)
				.toString();

		session.execute(insert);

		String select = QueryBuilder
				.select("firstname", "lastname", "age")
				.from("cql3_user")
				.where(QueryBuilder.eq("id", userId))
				.toString();

		Row row = session.execute(select).all().get(0);

		assertThat(row.getString("firstname")).isEqualTo("FN");
		assertThat(row.getString("lastname")).isNull();
		assertThat(row.getInt("age")).isEqualTo(0);

	}

	@Test
	public void should_batch_insert_with_different_consistency_levels() throws Exception
	{
		String insert1 = QueryBuilder.insertInto("cql3_user") //
				.value("id", 100001L)
				.value("firstname", "FN1")
				.value("lastname", "LN1")
				.value("age", 31)
				.toString();

		String insert2 = QueryBuilder.insertInto("cql3_user") //
				.value("id", 100002L)
				.value("firstname", "FN2")
				.value("lastname", "LN2")
				.value("age", 32)
				.toString();

		String insert3 = QueryBuilder.insertInto("cql3_user") //
				.value("id", 100003L)
				.value("firstname", "FN3")
				.value("lastname", "LN3")
				.value("age", 33)
				.toString();

		Query batch = QueryBuilder //
				.batch(new SimpleStatement(insert1))
				.add(new SimpleStatement(insert2))
				.add(new SimpleStatement(insert3))
				.setConsistencyLevel(ConsistencyLevel.ALL)
				.enableTracing();

		ResultSet resultSet = session.execute(batch);
		resultSet.all();

	}

	@Test
	public void should_execute_prepared_statement() throws Exception
	{

		PreparedStatement preparedStatement = session
				.prepare("INSERT INTO cql3_user(id,firstname,lastname,age) VALUES (?,?,?,?)");

		BoundStatement boundStatement = preparedStatement.bind(100004L, "FN4", "LN4", new Integer(
				34));
		boundStatement.setConsistencyLevel(ConsistencyLevel.ANY);

		session.execute(boundStatement);
	}

	@Test
	public void should_query_with_clustered_id() throws Exception
	{
		for (int i = 1; i <= 5; i++)
		{
			for (int j = 1; j <= 5; j++)
			{
				for (int k = 1; k <= 5; k++)
				{
					for (int l = 1; l <= 2; l++)
					{
						String insert = QueryBuilder.insertInto("clustering") //
								.value("a", i)
								.value("b", j)
								.value("c", k)
								.value("d", l)
								.toString();
						session.execute(insert);
					}
				}
			}
		}

		PreparedStatement preparedStatement1 = session
				.prepare("select d from clustering where a=? and b=? and c>?");
		BoundStatement boundStatement1 = preparedStatement1.bind(1, 1, 2);
		List<Row> rows = session.execute(boundStatement1).all();
		assertThat(rows.size()).isEqualTo(3);
	}

	@Test
	public void should_insert_twice_with_prepared_statement() throws Exception
	{
		long id = RandomUtils.nextLong();
		PreparedStatement preparedStatement = session
				.prepare("INSERT INTO cql3_user(id,firstname,lastname,age) VALUES (?,?,?,?)");

		BoundStatement boundStatement = preparedStatement.bind(id, "FN1", "LN1", 11);
		session.execute(boundStatement);

		BoundStatement boundStatement2 = preparedStatement.bind(id, "FN1", null, null);
		session.execute(boundStatement2);

		Row row = session.execute("select * from cql3_user where id=" + id).one();

		assertThat(row.getString("firstname")).isEqualTo("FN1");
		assertThat(row.isNull("lastname")).isTrue();
		assertThat(row.isNull("age")).isTrue();
	}

	@Test
	public void should_find_primary_key() throws Exception
	{
		long id = RandomUtils.nextLong();
		String insert = QueryBuilder.insertInto("widerow") //
				.value("id", id)
				.value("key", "k1")
				.value("value", "v1")
				.toString();
		session.execute(insert);

		String select = QueryBuilder
				.select("id")
				.from("widerow")
				.where(QueryBuilder.eq("id", id))
				.toString();

		assertThat(session.execute(select).all()).hasSize(1);
	}

	@Test
	public void should_delete_wide_row_by_partition_key() throws Exception
	{
		long id = RandomUtils.nextLong();

		String insert1 = QueryBuilder.insertInto("widerow") //
				.value("id", id)
				.value("key", "k1")
				.value("value", "v1")
				.toString();

		String insert2 = QueryBuilder.insertInto("widerow") //
				.value("id", id)
				.value("key", "k2")
				.value("value", "v2")
				.toString();

		session.execute(insert1);
		session.execute(insert2);

		String select = QueryBuilder
				.select("key", "value")
				.from("widerow")
				.where(QueryBuilder.eq("id", id))
				.toString();

		List<Row> rows = session.execute(select).all();
		assertThat(rows).hasSize(2);

		Query delete = QueryBuilder.delete().from("widerow").where(QueryBuilder.eq("id", id));
		session.execute(delete);

		rows = session.execute(select).all();
		assertThat(rows).isEmpty();

	}

	@Test
	public void should_return_empty_row_when_no_entity_found() throws Exception
	{
		long id = RandomUtils.nextLong();
		String select = QueryBuilder
				.select("myList")
				.from("cql3_list")
				.where(QueryBuilder.eq("id", id))
				.toString();

		List<Row> rows = session.execute(select).all();

		assertThat(rows).isEmpty();
	}

	@Test
	public void should_prepare_statement_for_counter() throws Exception
	{
		String incr = "UPDATE achillescounter SET counter_value = counter_value + ? WHERE fqcn=? AND pk=? and key=?";

		PreparedStatement ps = session.prepare(incr);
		BoundStatement bs = ps.bind(2L, "CompleteBean", "150", "clicks");

		session.execute(bs);

		String select = "SELECT counter_value from achillescounter WHERE fqcn='CompleteBean' AND pk='150' and key='clicks'";
		List<Row> rows = session.execute(select).all();

		assertThat(rows).hasSize(1);
		assertThat(rows.get(0).getLong("counter_value")).isEqualTo(2L);

		session
				.execute("DELETE FROM achillescounter  WHERE fqcn='CompleteBean' AND pk='150' and key='clicks'");

		rows = session.execute(select).all();
		assertThat(rows).isEmpty();

	}

	@Test
	public void should_select_clustered_key_with_IN_clause() throws Exception
	{
		// clustering(a int,b int,c int,d int, primary key (a,b,c)
		for (int i = 1; i <= 5; i++)
		{
			for (int j = 1; j <= 5; j++)
			{
				for (int k = 1; k <= 5; k++)
				{
					for (int l = 1; l <= 2; l++)
					{
						String insert = QueryBuilder.insertInto("clustering") //
								.value("a", i)
								.value("b", j)
								.value("c", k)
								.value("d", l)
								.toString();
						session.execute(insert);
					}
				}
			}
		}

		List<Row> rows = session.execute(
				"SELECT * FROM clustering WHERE a IN(1,4) AND b>=2 AND b<=3").all();

		assertThat(rows).hasSize(20);

		rows = session
				.execute("SELECT * FROM clustering WHERE a IN(1,4) ORDER BY b,c LIMIT 49")
				.all();

		assertThat(rows).hasSize(49);
	}

	@After
	public void cleanUp()
	{
		truncateTables();
	}
}
