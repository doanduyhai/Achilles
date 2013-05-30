package integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
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

	@Before
	public void setUp()
	{
		String tableUser = "create table cql3_user(id bigint,firstname text,lastname text, age int, primary key(id))";
		String tableList = "create table cql3_list(id bigint,myList list<text>, primary key(id))";
		String tableSet = "create table cql3_set(id bigint,mySet set<text>, primary key(id))";
		String tableMap = "create table cql3_map(id bigint,myMap map<int,text>, primary key(id))";
		String wideRow = "create table widerow(id bigint,key text,value text, primary key(id,key))";
		session.execute(tableUser);
		session.execute(tableList);
		session.execute(tableSet);
		session.execute(tableMap);
		session.execute(wideRow);
	}

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
		assertThat(myList).hasSize(3);
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
				.value("lastname", "LN")
				.value("age", 35)
				.toString();

		session.execute(insert);

		insert = QueryBuilder.insertInto("cql3_user") //
				.value("id", userId)
				.value("firstname", "FN2")
				.toString();

		session.execute(insert);
		Update update = QueryBuilder.update("cql3_user");
		update.with(QueryBuilder.set("lastname", "LN2"));
		update.where(QueryBuilder.eq("id", userId));

		session.execute(update);

		Clause filterById = QueryBuilder.eq("id", userId);
		String select = QueryBuilder
				.select("firstname", "lastname", "age")
				.from("cql3_user")
				.where(filterById)
				.toString();

		Row row = session.execute(select).all().get(0);

		assertThat(row.getString("firstname")).isEqualTo("FN2");
		assertThat(row.getString("lastname")).isEqualTo("LN2");
		assertThat(row.getInt("age")).isEqualTo(35);

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

		ResultSet result = session.execute(boundStatement);

		System.out.println(result.getExecutionInfo().getQueriedHost());
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

	@After
	public void cleanUp()
	{
		String listAllTables = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='achilles'";
		List<Row> rows = session.execute(listAllTables).all();

		for (Row row : rows)
		{
			session
					.execute(new SimpleStatement("drop table " + row.getString("columnfamily_name")));
		}
	}
}
