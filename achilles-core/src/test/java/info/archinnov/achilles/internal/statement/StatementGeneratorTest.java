/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.statement;

import static com.datastax.driver.core.querybuilder.Update.Assignments;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.*;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.APPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ASSIGN_VALUE_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ASSIGN_VALUE_TO_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ASSIGN_VALUE_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.PREPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_COLLECTION_OR_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;
import static info.archinnov.achilles.test.builders.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.test.builders.PropertyMetaTestBuilder.completeBean;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update.Where;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class StatementGeneratorTest {

	@InjectMocks
	private StatementGenerator generator;

	@Mock
	private SliceQueryStatementGenerator sliceQueryGenerator;


	@Mock
	private CQLSliceQuery<ClusteredEntity> sliceQuery;

	@Mock
	private DaoContext daoContext;

	@Mock
	private RegularStatementWrapper statementWrapper;

    @Mock
    private DirtyCheckChangeSet dirtyCheckChangeSet;

	@Captor
	private ArgumentCaptor<RegularStatement> statementCaptor;

	@Captor
	private ArgumentCaptor<Select> selectCaptor;

	@Captor
	private ArgumentCaptor<Delete> deleteCaptor;

	private ReflectionInvoker invoker = new ReflectionInvoker();

	@Test
	public void should_create_select_statement_for_entity_simple_id() throws Exception {
		EntityMeta meta = prepareEntityMeta("id");

		RegularStatement statement = generator.generateSelectEntity(meta);

		assertThat(statement.getQueryString()).isEqualTo("SELECT id,age,name,label FROM table;");
	}

	@Test
	public void should_create_select_statement_for_entity_compound_id() throws Exception {

		EntityMeta meta = prepareEntityMeta("id", "a", "b");

		RegularStatement statement = generator.generateSelectEntity(meta);

		assertThat(statement.getQueryString()).isEqualTo("SELECT id,a,b,age,name,label FROM table;");
	}

	@Test
	public void should_generate_slice_select_query() throws Exception {
		EntityMeta meta = prepareEntityMeta("id", "comp1", "comp2");
		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQuery.getCQLOrdering()).thenReturn(QueryBuilder.desc("comp1"));
		when(sliceQuery.getConsistencyLevel()).thenReturn(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		when(sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(eq(sliceQuery), selectCaptor.capture()))
				.thenReturn(statementWrapper);
		RegularStatementWrapper actual = generator.generateSelectSliceQuery(sliceQuery, 98,101);

		assertThat(actual).isSameAs(statementWrapper);

		assertThat(selectCaptor.getValue().getQueryString()).isEqualTo(
				"SELECT id,comp1,comp2,age,name,label FROM table ORDER BY comp1 DESC LIMIT 98;");
        assertThat(selectCaptor.getValue().getFetchSize()).isEqualTo(101);
	}

    @Test
    public void should_generate_insert_for_simple_id() throws Exception {
        //Given
        Long primaryKey = RandomUtils.nextLong();
        final String myName = "myName";
        CompleteBean entity = new CompleteBean();
        EntityMeta meta = mock(EntityMeta.class);
        PropertyMeta idMeta = mock(PropertyMeta.class);
        PropertyMeta nameMeta = mock(PropertyMeta.class);

        //When
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.getTableName()).thenReturn("table");

        when(idMeta.isEmbeddedId()).thenReturn(false);
        when(idMeta.getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.encode(primaryKey)).thenReturn(primaryKey);
        when(idMeta.getPropertyName()).thenReturn("id");

        when(meta.getColumnsMetaToInsert()).thenReturn(asList(nameMeta));

        when(nameMeta.type()).thenReturn(PropertyType.SIMPLE);
        when(nameMeta.getAndEncodeValueForCassandra(entity)).thenReturn(myName);
        when(nameMeta.getPropertyName()).thenReturn("name");

        final Pair<Insert, Object[]> pair = generator.generateInsert(entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("INSERT INTO table(id,name) VALUES (" + primaryKey + ",?);");
        assertThat(asList(pair.right)).containsExactly(primaryKey, myName);
    }

    @Test
    public void should_generate_insert_for_composite_partition_key() throws Exception {
        //Given
        Object primaryKey = new Object();
        Long id = RandomUtils.nextLong();
        String type = "type";
        final String myName = "myName";
        CompleteBean entity = new CompleteBean();
        EntityMeta meta = mock(EntityMeta.class);
        PropertyMeta idMeta = mock(PropertyMeta.class);
        PropertyMeta nameMeta = mock(PropertyMeta.class);

        //When
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.getTableName()).thenReturn("table");

        when(idMeta.isEmbeddedId()).thenReturn(true);
        when(idMeta.getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.getComponentNames()).thenReturn(asList("id", "type"));
        when(idMeta.encodeToComponents(primaryKey)).thenReturn(Arrays.<Object>asList(id,type));

        when(meta.getColumnsMetaToInsert()).thenReturn(asList(nameMeta));

        when(nameMeta.type()).thenReturn(PropertyType.SIMPLE);
        when(nameMeta.getAndEncodeValueForCassandra(entity)).thenReturn(myName);
        when(nameMeta.getPropertyName()).thenReturn("name");

        final Pair<Insert, Object[]> pair = generator.generateInsert(entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("INSERT INTO table(id,type,name) VALUES ("+id+",?,?);");
        assertThat(asList(pair.right)).containsExactly(id,type,myName);
    }

	@Test
	public void should_generate_slice_select_query_without_ordering() throws Exception {
		EntityMeta meta = prepareEntityMeta("id", "comp1", "comp2");
		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQuery.getCQLOrdering()).thenReturn(null);
		when(sliceQuery.getConsistencyLevel()).thenReturn(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		when(sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(eq(sliceQuery), selectCaptor.capture()))
				.thenReturn(statementWrapper);

		RegularStatementWrapper actual = generator.generateSelectSliceQuery(sliceQuery, 98,101);

		assertThat(actual).isSameAs(statementWrapper);
		assertThat(selectCaptor.getValue().getQueryString()).isEqualTo(
				"SELECT id,comp1,comp2,age,name,label FROM table LIMIT 98;");
        assertThat(selectCaptor.getValue().getFetchSize()).isEqualTo(101);
	}

	@Test
	public void should_generate_slice_delete_query() throws Exception {
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQueryGenerator.generateWhereClauseForDeleteSliceQuery(eq(sliceQuery), deleteCaptor.capture()))
				.thenReturn(statementWrapper);

		RegularStatementWrapper actual = generator.generateRemoveSliceQuery(sliceQuery);

		assertThat(actual).isSameAs(statementWrapper);
		assertThat(deleteCaptor.getValue().getQueryString()).isEqualTo("DELETE  FROM table;");
	}

	@Test
	public void should_generate_update_for_simple_id() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
				.type(ID).invoker(invoker).build();

		PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").accessors()
				.type(SIMPLE).invoker(invoker).build();

		PropertyMeta friendsMeta = completeBean(Void.class, String.class).field("friends")
				.accessors().type(LIST).invoker(invoker).build();

		PropertyMeta followersMeta = completeBean(Void.class, String.class).field("followers")
				.accessors().type(SET).invoker(invoker).build();

		PropertyMeta preferencesMeta = completeBean(Integer.class, String.class)
				.field("preferences").accessors().type(MAP).invoker(invoker).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "age", ageMeta, "followers", followersMeta, "preferences",
				preferencesMeta));
		meta.setIdMeta(idMeta);

		Long id = RandomUtils.nextLong();
		Long age = RandomUtils.nextLong();

		List<String> friends = asList("foo", "bar");

		Set<String> followers = new TreeSet<>();
		followers.add("john");
		followers.add("helen");

		Map<Integer, String> preferences = ImmutableMap.of(1, "FR", 2, "Paris");

		CompleteBean entity = builder().id(id).age(age).addFriends(friends)
				.addFollowers(followers).addPreferences(preferences).buid();

		Pair<Where, Object[]> pair = generator.generateUpdateFields(entity, meta,
				asList(ageMeta, friendsMeta, followersMeta, preferencesMeta));

		assertThat(pair.left.getQueryString()).isEqualTo(
				"UPDATE table SET age=" + age + ",friends=?,followers=?,preferences=? WHERE id=" + id + ";");

		assertThat(pair.right).contains(age, friends, followers, preferences, id);
	}

	@Test
	public void should_generate_update_for_clustered_id() throws Exception {
		Field idField = ClusteredEntity.class.getDeclaredField("id");
        Field valueField = ClusteredEntity.class.getDeclaredField("value");
        Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
        Field nameField = EmbeddedKey.class.getDeclaredField("name");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compNames("id", "name")
				.compClasses(Long.class, String.class).compFields(userIdField, nameField).field("id")
				.type(EMBEDDED_ID).invoker(invoker).build();
        idMeta.setField(idField);

		PropertyMeta valueMeta = PropertyMetaTestBuilder.valueClass(String.class).field("value").type(SIMPLE)
				.invoker(invoker).build();
        valueMeta.setField(valueField);

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

		Pair<Where, Object[]> pair = generator.generateUpdateFields(entity, meta, asList(valueMeta));

		assertThat(pair.left.getQueryString())
				.isEqualTo("UPDATE table SET value=? WHERE id=" + userId + " AND name=?;");
		assertThat(pair.right).contains("value", userId, "name");

	}

    @Test
    public void should_generate_remove_all_to_collection_and_map_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_COLLECTION_OR_MAP);
        when(dirtyCheckChangeSet.generateUpdateForRemoveAll(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_assign_value_to_set() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_SET);
        when(dirtyCheckChangeSet.generateUpdateForAssignValueToSet(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_assign_value_to_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_LIST);
        when(dirtyCheckChangeSet.generateUpdateForAssignValueToList(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_assign_value_to_map() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_MAP);
        when(dirtyCheckChangeSet.generateUpdateForAssignValueToMap(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_add_element_to_set_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(ADD_TO_SET);
        when(dirtyCheckChangeSet.generateUpdateForAddedElements(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_remove_element_from_set_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_FROM_SET);
        when(dirtyCheckChangeSet.generateUpdateForRemovedElements(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_append_element_to_list_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(APPEND_TO_LIST);
        when(dirtyCheckChangeSet.generateUpdateForAppendedElements(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_prepend_element_to_list_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(PREPEND_TO_LIST);
        when(dirtyCheckChangeSet.generateUpdateForPrependedElements(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_remove_element_from_list_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST);
        when(dirtyCheckChangeSet.generateUpdateForRemoveListElements(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_set_element_at_index_to_list_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(SET_TO_LIST_AT_INDEX);
        when(dirtyCheckChangeSet.generateUpdateForSetAtIndexElement(any(Assignments.class))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_remove_element_at_index_to_list_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST_AT_INDEX);
        when(dirtyCheckChangeSet.generateUpdateForRemovedAtIndexElement(any(Assignments.class))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_add_entries_to_map_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(ADD_TO_MAP);
        when(dirtyCheckChangeSet.generateUpdateForAddedEntries(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

    @Test
    public void should_generate_remove_entry_from_map_update() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors()
                .type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
        meta.setIdMeta(idMeta);

        Long id = RandomUtils.nextLong();
        Object[] boundValues = new Object[]{"whatever"};
        CompleteBean entity = builder().id(id).buid();

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_FROM_MAP);
        when(dirtyCheckChangeSet.generateUpdateForRemovedKey(any(Assignments.class), eq(false))).thenReturn(boundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(dirtyCheckChangeSet, entity, meta);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table WHERE id="+id+";");
        assertThat(pair.right[0]).isEqualTo("whatever");
    }

	private EntityMeta prepareEntityMeta(String... componentNames) throws Exception {
		PropertyMeta idMeta;
		if (componentNames.length > 1) {
			idMeta = completeBean(Void.class, EmbeddedKey.class).field("id")
					.compNames(componentNames).type(PropertyType.EMBEDDED_ID).build();
		} else {
			idMeta = completeBean(Void.class, Long.class).field(componentNames[0]).type(ID)
					.build();
		}

		PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").type(SIMPLE)
				.build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).build();

		PropertyMeta labelMeta = completeBean(Void.class, String.class).field("label")
				.type(SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");
		meta.setAllMetasExceptCounters(asList(idMeta, ageMeta, nameMeta, labelMeta));
		meta.setAllMetasExceptId(asList(ageMeta, nameMeta, labelMeta));
		meta.setAllMetasExceptIdAndCounters(asList(ageMeta, nameMeta, labelMeta));
		meta.setIdMeta(idMeta);

		return meta;
	}


}
