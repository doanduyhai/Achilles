package info.archinnov.achilles.statement;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.type.WideMap.BoundingMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * CQLStringStatementGeneratorTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLStatementGeneratorTest
{

	private CQLStatementGenerator generator = new CQLStatementGenerator();

	@Test
	public void should_create_select_statement_for_entity_simple_id() throws Exception
	{
		List<PropertyMeta<?, ?>> eagerMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("age")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> labelMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("label")
				.type(PropertyType.SIMPLE)
				.build();

		eagerMetas.add(ageMeta);
		eagerMetas.add(nameMeta);
		eagerMetas.add(labelMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setEagerMetas(eagerMetas);
		meta.setIdMeta(idMeta);

		Select select = generator.generateSelectEntity(meta);

		assertThat(select.getQueryString()).isEqualTo("SELECT id,age,name,label FROM table;");
	}

	@Test
	public void should_create_select_statement_for_entity_compound_id() throws Exception
	{
		List<PropertyMeta<?, ?>> eagerMetas = new ArrayList<PropertyMeta<?, ?>>();

		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.compNames("id", "a", "b")
				.type(PropertyType.COMPOUND_KEY)
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("age")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> labelMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("label")
				.type(PropertyType.SIMPLE)
				.build();

		eagerMetas.add(ageMeta);
		eagerMetas.add(nameMeta);
		eagerMetas.add(labelMeta);
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setEagerMetas(eagerMetas);
		meta.setIdMeta(idMeta);

		Select select = generator.generateSelectEntity(meta);

		assertThat(select.getQueryString()).isEqualTo("SELECT id,a,b,age,name,label FROM table;");
	}

	@Test
	public void should_generate_where_clause_for_slice_query() throws Exception
	{

		List<String> componentNames = Arrays.asList("a", "b", "c");
		UUID uuid1 = new UUID(10, 11);

		// /////////////////////////// Same number of components
		List<Object> startValues = Arrays.<Object> asList(uuid1, "author", 1);
		List<Object> endValues = Arrays.<Object> asList(uuid1, "author", 2);

		Statement statement = generator.generateWhereClauseForSliceQuery(componentNames,
				startValues, endValues, BoundingMode.INCLUSIVE_BOUNDS, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>=1 AND c<=2;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.EXCLUSIVE_BOUNDS, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>1 AND c<2;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_START_BOUND_ONLY, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>=1 AND c<2;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_END_BOUND_ONLY, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>1 AND c<=2;");

		// ///////////////////// More components for start compound key
		startValues = Arrays.<Object> asList(uuid1, "author", 1);
		endValues = Arrays.<Object> asList(uuid1, "author", null);

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_BOUNDS, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>=1;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.EXCLUSIVE_BOUNDS, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>1;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_START_BOUND_ONLY, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>=1;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_END_BOUND_ONLY, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c>1;");

		// ///////////////////// More components for end compound key
		startValues = Arrays.<Object> asList(uuid1, "author", null);
		endValues = Arrays.<Object> asList(uuid1, "author", 1);

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_BOUNDS, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c<=1;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.EXCLUSIVE_BOUNDS, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c<1;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_START_BOUND_ONLY, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c<1;");

		statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues,
				endValues, BoundingMode.INCLUSIVE_END_BOUND_ONLY, buildFakeSelect());

		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE a=" + uuid1 + " AND b='author' AND c<=1;");

	}

	private Select buildFakeSelect()
	{
		Select select = QueryBuilder.select("test").from("table");
		return select;
	}
}
