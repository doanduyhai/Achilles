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
package info.archinnov.achilles.statement;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.prepared.SliceQueryPreparedStatementGenerator;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class StatementGeneratorTest {

	@InjectMocks
	private StatementGenerator generator;

	@Mock
	private SliceQueryStatementGenerator sliceQueryGenerator;

	@Mock
	private SliceQueryPreparedStatementGenerator sliceQueryPreparedGenerator;

	@Mock
	private CQLSliceQuery<ClusteredEntity> sliceQuery;

	@Mock
	private DaoContext daoContext;

	private ReflectionInvoker invoker = new ReflectionInvoker();

	@Captor
	private ArgumentCaptor<Statement> statementCaptor;

	@Test
	public void should_create_select_statement_for_entity_simple_id() throws Exception {
		EntityMeta meta = prepareEntityMeta("id");

		Select select = generator.generateSelectEntity(meta);

		assertThat(select.getQueryString()).isEqualTo("SELECT id,age,name,label FROM table;");
	}

	@Test
	public void should_create_select_statement_for_entity_compound_id() throws Exception {

		EntityMeta meta = prepareEntityMeta("id", "a", "b");

		Select select = generator.generateSelectEntity(meta);

		assertThat(select.getQueryString()).isEqualTo("SELECT id,a,b,age,name,label FROM table;");
	}

	@Test
	public void should_generate_slice_select_query() throws Exception {
		EntityMeta meta = prepareEntityMeta("id", "comp1", "comp2");
		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQuery.getCQLOrdering()).thenReturn(QueryBuilder.desc("comp1"));
		when(sliceQuery.getConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);
		when(sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(eq(sliceQuery), any(Select.class))).thenAnswer(
				new Answer<Statement>() {

					@Override
					public Statement answer(InvocationOnMock invocation) throws Throwable {
						return buildFakeWhereForSelect((Select) invocation.getArguments()[1]);
					}
				});

		Query query = generator.generateSelectSliceQuery(sliceQuery, 98);

		assertThat(query.toString()).isEqualTo(
				"SELECT id,comp1,comp2,age,name,label FROM table WHERE fake='fake' ORDER BY comp1 DESC LIMIT 98;");
	}
	
	@Test
	public void should_generate_slice_select_query_without_ordering() throws Exception {
		EntityMeta meta = prepareEntityMeta("id", "comp1", "comp2");
		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQuery.getCQLOrdering())
				.thenReturn(null);
		when(sliceQuery.getConsistencyLevel()).thenReturn(
				ConsistencyLevel.EACH_QUORUM);
		when(
				sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(
						eq(sliceQuery), any(Select.class))).thenAnswer(
				new Answer<Statement>() {

					@Override
					public Statement answer(InvocationOnMock invocation)
							throws Throwable {
						return buildFakeWhereForSelect((Select) invocation
								.getArguments()[1]);
					}
				});

		Query query = generator.generateSelectSliceQuery(sliceQuery, 98);

		assertThat(query.toString())
				.isEqualTo(
						"SELECT id,comp1,comp2,age,name,label FROM table WHERE fake='fake' LIMIT 98;");
	}

	@Test
	public void should_generate_slice_iterator_query() throws Exception {
		EntityMeta meta = prepareEntityMeta("id", "comp1", "comp2");
		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQuery.getLimit()).thenReturn(99);
		when(sliceQuery.getCQLOrdering()).thenReturn(QueryBuilder.desc("comp1"));
		when(sliceQuery.getConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);
		when(sliceQueryPreparedGenerator.generateWhereClauseForIteratorSliceQuery(eq(sliceQuery), any(Select.class)))
				.thenAnswer(new Answer<Statement>() {

					@Override
					public Statement answer(InvocationOnMock invocation) throws Throwable {
						return buildFakeWhereForSelect((Select) invocation.getArguments()[1]);
					}
				});
		PreparedStatement ps = mock(PreparedStatement.class);

		when(daoContext.prepare(statementCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = generator.generateIteratorSliceQuery(sliceQuery, daoContext);

		assertThat(actual).isSameAs(ps);

		assertThat(statementCaptor.getValue().getQueryString()).isEqualTo(
				"SELECT id,comp1,comp2,age,name,label FROM table WHERE fake='fake' ORDER BY comp1 DESC LIMIT 99;");

		verify(ps).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_generate_slice_delete_query() throws Exception {
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQueryGenerator.generateWhereClauseForDeleteSliceQuery(eq(sliceQuery), any(Delete.class))).thenAnswer(
				new Answer<Statement>() {
					@Override
					public Statement answer(InvocationOnMock invocation) throws Throwable {
						return buildFakeWhereForDelete((Delete) invocation.getArguments()[1]);
					}
				});

		Query query = generator.generateRemoveSliceQuery(sliceQuery);

		assertThat(query.toString()).isEqualTo("DELETE  FROM table WHERE fake='fake';");
	}

	@Test
	public void should_generate_insert_for_simple_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors()
				.type(ID).invoker(invoker).build();

		PropertyMeta ageMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("age").accessors()
				.type(SIMPLE).invoker(invoker).build();

		PropertyMeta followersMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("followers")
				.accessors().type(SET).invoker(invoker).build();

		PropertyMeta preferencesMeta = PropertyMetaTestBuilder.completeBean(Integer.class, String.class)
				.field("preferences").accessors().type(MAP).invoker(invoker).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setAllMetasExceptIdMeta(Arrays.asList(ageMeta, followersMeta, preferencesMeta));
		meta.setIdMeta(idMeta);

		Long id = RandomUtils.nextLong();
		Long age = RandomUtils.nextLong();
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(id).age(age).addFollowers("john", "helen")
				.addPreference(1, "FR").addPreference(2, "Paris").buid();

		Insert insert = generator.generateInsert(entity, meta);

		assertThat(insert.getQueryString()).isEqualTo(
				"INSERT INTO table(id,age,followers,preferences) VALUES (" + id + "," + age
						+ ",{'helen','john'},{1:'FR',2:'Paris'});");

	}

	@Test
	public void should_generate_insert_for_clustered_id() throws Exception {
		Method idGetter = ClusteredEntity.class.getDeclaredMethod("getId");
		Method valueGetter = ClusteredEntity.class.getDeclaredMethod("getValue");
		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = EmbeddedKey.class.getDeclaredMethod("getName");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compNames("id", "name")
				.compClasses(Long.class, String.class).compGetters(userIdGetter, nameGetter).field("id")
				.type(EMBEDDED_ID).invoker(invoker).build();
		idMeta.setGetter(idGetter);

		PropertyMeta valueMeta = PropertyMetaTestBuilder.valueClass(String.class).field("value").type(SIMPLE)
				.invoker(invoker).build();
		valueMeta.setGetter(valueGetter);

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setAllMetasExceptIdMeta(Arrays.asList(valueMeta));
		meta.setIdMeta(idMeta);

		Long userId = RandomUtils.nextLong();
		ClusteredEntity entity = new ClusteredEntity();
		EmbeddedKey embeddedKey = new EmbeddedKey();
		embeddedKey.setUserId(userId);
		embeddedKey.setName("name");
		entity.setId(embeddedKey);
		entity.setValue("value");

		Insert insert = generator.generateInsert(entity, meta);

		assertThat(insert.getQueryString()).isEqualTo(
				"INSERT INTO table(id,name,value) VALUES (" + userId + ",'name','value');");

	}

	@Test
	public void should_generate_update_for_simple_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors()
				.type(ID).invoker(invoker).build();

		PropertyMeta ageMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("age").accessors()
				.type(SIMPLE).invoker(invoker).build();

		PropertyMeta friendsMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends")
				.accessors().type(LAZY_LIST).invoker(invoker).build();

		PropertyMeta followersMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("followers")
				.accessors().type(SET).invoker(invoker).build();

		PropertyMeta preferencesMeta = PropertyMetaTestBuilder.completeBean(Integer.class, String.class)
				.field("preferences").accessors().type(MAP).invoker(invoker).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "age", ageMeta, "followers", followersMeta, "preferences",
				preferencesMeta));
		meta.setIdMeta(idMeta);

		Long id = RandomUtils.nextLong();
		Long age = RandomUtils.nextLong();
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(id).age(age).addFriends("foo", "bar")
				.addFollowers("john", "helen").addPreference(1, "FR").addPreference(2, "Paris").buid();

		Update.Assignments update = generator.generateUpdateFields(entity, meta,
				Arrays.asList(ageMeta, friendsMeta, followersMeta, preferencesMeta));

		assertThat(update.getQueryString()).isEqualTo(
				"UPDATE table SET age=" + age
						+ ",friends=['foo','bar'],followers={'helen','john'},preferences={1:'FR',2:'Paris'} WHERE id="
						+ id + ";");

	}

	@Test
	public void should_generate_update_for_clustered_id() throws Exception {
		Method idGetter = ClusteredEntity.class.getDeclaredMethod("getId");
		Method valueGetter = ClusteredEntity.class.getDeclaredMethod("getValue");
		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = EmbeddedKey.class.getDeclaredMethod("getName");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compNames("id", "name")
				.compClasses(Long.class, String.class).compGetters(userIdGetter, nameGetter).field("id")
				.type(EMBEDDED_ID).invoker(invoker).build();
		idMeta.setGetter(idGetter);

		PropertyMeta valueMeta = PropertyMetaTestBuilder.valueClass(String.class).field("value").type(SIMPLE)
				.invoker(invoker).build();
		valueMeta.setGetter(valueGetter);

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "value", valueMeta));
		meta.setIdMeta(idMeta);

		Long userId = RandomUtils.nextLong();
		ClusteredEntity entity = new ClusteredEntity();
		EmbeddedKey embeddedKey = new EmbeddedKey();
		embeddedKey.setUserId(userId);
		embeddedKey.setName("name");
		entity.setId(embeddedKey);
		entity.setValue("value");

		Update.Assignments update = generator.generateUpdateFields(entity, meta, Arrays.asList(valueMeta));

		assertThat(update.getQueryString()).isEqualTo(
				"UPDATE table SET value='value' WHERE id=" + userId + " AND name='name';");

	}

	private EntityMeta prepareEntityMeta(String... componentNames) throws Exception {
		PropertyMeta idMeta;
		if (componentNames.length > 1) {
			idMeta = PropertyMetaTestBuilder.completeBean(Void.class, EmbeddedKey.class).field("id")
					.compNames(componentNames).type(PropertyType.EMBEDDED_ID).build();
		} else {
			idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field(componentNames[0]).type(ID)
					.build();
		}

		PropertyMeta ageMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("age").type(SIMPLE)
				.build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).build();

		PropertyMeta labelMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("label")
				.type(SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setEagerMetas(Arrays.asList(idMeta, ageMeta, nameMeta, labelMeta));
		meta.setIdMeta(idMeta);

		return meta;
	}

	private Statement buildFakeWhereForSelect(Select select) {
		return select.where().and(QueryBuilder.eq("fake", "fake"));
	}

	private Statement buildFakeWhereForDelete(Delete delete) {
		return delete.where().and(QueryBuilder.eq("fake", "fake"));
	}
}
