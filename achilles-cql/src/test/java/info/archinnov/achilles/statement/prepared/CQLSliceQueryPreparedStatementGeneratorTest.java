/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.statement.prepared;

import static info.archinnov.achilles.type.BoundingMode.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

@RunWith(MockitoJUnitRunner.class)
public class CQLSliceQueryPreparedStatementGeneratorTest {

	private CQLSliceQueryPreparedStatementGenerator generator = new CQLSliceQueryPreparedStatementGenerator();

	@Mock
	private CQLSliceQuery<ClusteredEntity> sliceQuery;

	private UUID uuid1 = new UUID(10, 11);

	private List<String> componentNames = Arrays.asList("id", "a", "b", "c");

	@Before
	public void setUp() {
		when(sliceQuery.getComponentNames()).thenReturn(componentNames);
	}

	// /////////////////////// ASCENDING
	@Test
	public void should_generate_iterator_where_clause_for_ascending() throws Exception {
		when(sliceQuery.getOrdering()).thenReturn(ASCENDING);
		when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
		when(sliceQuery.getLastStartComponent()).thenReturn(1);
		when(sliceQuery.getLastEndComponent()).thenReturn(2);

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
		Statement statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>? AND c<=2;");

		when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>? AND c<2;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>? AND c<2;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>? AND c<=2;");
	}

	@Test
	public void should_generate_iterator_where_clause_with_no_end_for_ascending() throws Exception {
		when(sliceQuery.getOrdering()).thenReturn(ASCENDING);
		when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
		when(sliceQuery.getLastStartComponent()).thenReturn(1);
		when(sliceQuery.getLastEndComponent()).thenReturn(null);

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
		Statement statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>?;");

		when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>?;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>?;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c>?;");
	}

	// /////////////////////// DESCENDING
	@Test
	public void should_generate_iterator_where_clause_for_descending() throws Exception {
		when(sliceQuery.getOrdering()).thenReturn(DESCENDING);
		when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
		when(sliceQuery.getLastStartComponent()).thenReturn(2);
		when(sliceQuery.getLastEndComponent()).thenReturn(1);

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
		Statement statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<? AND c>=1;");

		when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<? AND c>1;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<? AND c>1;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<? AND c>=1;");
	}

	@Test
	public void should_generate_iterator_where_clause_with_no_end_for_descending() throws Exception {
		when(sliceQuery.getOrdering()).thenReturn(DESCENDING);
		when(sliceQuery.getFixedComponents()).thenReturn(Arrays.<Object> asList(11L, uuid1, "author"));
		when(sliceQuery.getLastStartComponent()).thenReturn(1);
		when(sliceQuery.getLastEndComponent()).thenReturn(null);

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_BOUNDS);
		Statement statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<?;");

		when(sliceQuery.getBounding()).thenReturn(EXCLUSIVE_BOUNDS);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<?;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_START_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<?;");

		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);
		statement = generator.generateWhereClauseForIteratorSliceQuery(sliceQuery, buildFakeSelect());
		assertThat(statement.getQueryString()).isEqualTo(
				"SELECT test FROM table WHERE id=11 AND a=" + uuid1 + " AND b='author' AND c<?;");
	}

	private Select buildFakeSelect() {
		Select select = QueryBuilder.select("test").from("table");
		return select;
	}
}
