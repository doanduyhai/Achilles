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
package info.archinnov.achilles.internal.statement.prepared;

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
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
import static info.archinnov.achilles.test.builders.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.Options.CASCondition;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

@RunWith(MockitoJUnitRunner.class)
public class PreparedStatementBinderTest {

    @InjectMocks
    private PreparedStatementBinder binder;

    private static final Optional<Integer> NO_TTL = Optional.absent();
    private static final Optional<Long> NO_TIMESTAMP = Optional.absent();
    private static final Optional<info.archinnov.achilles.type.ConsistencyLevel> NO_CONSISTENCY = Optional.absent();

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private PreparedStatement ps;

    @Mock
    private BoundStatement bs;

    @Mock
    private DataTranscoder transcoder;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private DirtyCheckChangeSet changeSet;

    @Mock
    private PersistenceContext.StateHolderFacade context;

    @Mock
    private ConsistencyOverrider overrider;

    private EntityMeta entityMeta;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
    public void setUp() {
        entityMeta = new EntityMeta();
        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(context.getTtl()).thenReturn(NO_TTL);
        when(context.getTimestamp()).thenReturn(NO_TIMESTAMP);
        when(context.getConsistencyLevel()).thenReturn(NO_CONSISTENCY);
    }

    @Test
    public void should_bind_for_insert_with_simple_id() throws Exception {
        long primaryKey = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(SIMPLE).accessors()
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").type(SIMPLE).accessors()
                .transcoder(transcoder).invoker(invoker).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(asList(nameMeta, ageMeta));
        entityMeta.setClusteredCounter(false);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getField())).thenReturn(name);
        when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(age);

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(ageMeta, age)).thenReturn(age);

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForInsert(context, ps, asList(nameMeta, ageMeta));

        verify(bs).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey, name, age, 0);
    }

    @Test
    public void should_bind_for_insert_with_null_fields() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(SIMPLE).accessors()
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").type(SIMPLE).accessors()
                .transcoder(transcoder).invoker(invoker).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(asList(nameMeta, ageMeta));
        entityMeta.setClusteredCounter(false);

        long primaryKey = RandomUtils.nextLong();
        String name = "name";
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getField())).thenReturn(name);
        when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(null);

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(eq(ageMeta), any())).thenReturn(null);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForInsert(context, ps, asList(nameMeta, ageMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey, name, null, 0);
    }

    @Test
    public void should_bind_for_insert_with_compound_key() throws Exception {
        long userId = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";
        List<Object> friends = Arrays.<Object>asList("foo", "bar");
        Set<Object> followers = Sets.<Object>newHashSet("George", "Paul");
        Map<Object, Object> preferences = ImmutableMap.<Object, Object>of(1, "FR");

        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(EMBEDDED_ID)
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta friendsMeta = completeBean(Void.class, String.class).field("friends").type(LIST)
                .transcoder(transcoder).accessors().invoker(invoker).build();

        PropertyMeta followersMeta = completeBean(Void.class, Long.class).field("followers").type(SET)
                .transcoder(transcoder).accessors().invoker(invoker).build();

        PropertyMeta preferencesMeta = completeBean(Void.class, Long.class).field("preferences").type(MAP)
                .transcoder(transcoder).accessors().invoker(invoker).build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setAllMetasExceptIdAndCounters(asList(friendsMeta, followersMeta, preferencesMeta));
        entityMeta.setClusteredCounter(false);

        EmbeddedKey embeddedKey = new EmbeddedKey(userId, name);

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(embeddedKey);
        when(invoker.getValueFromField(entity, friendsMeta.getField())).thenReturn(friends);
        when(invoker.getValueFromField(entity, followersMeta.getField())).thenReturn(followers);
        when(invoker.getValueFromField(entity, preferencesMeta.getField())).thenReturn(preferences);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        when(transcoder.encodeToComponents(idMeta, embeddedKey)).thenReturn(Arrays.<Object>asList(userId, name));
        when(transcoder.encode(friendsMeta, friends)).thenReturn(friends);
        when(transcoder.encode(followersMeta, followers)).thenReturn(followers);
        when(transcoder.encode(preferencesMeta, preferences)).thenReturn(preferences);

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForInsert(context, ps, asList(friendsMeta, followersMeta, preferencesMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(userId, name, friends, followers, preferences, 0);
    }

    @Test
    public void should_bind_with_only_pk_in_where_clause() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
                .transcoder(transcoder).invoker(invoker).build();
        entityMeta.setIdMeta(idMeta);
        long primaryKey = RandomUtils.nextLong();

        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindStatementWithOnlyPKInWhereClause(context, ps, info.archinnov.achilles.type.ConsistencyLevel.ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey);
    }

    @Test
    public void should_bind_for_update() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").accessors().type(SIMPLE)
                .transcoder(transcoder).invoker(invoker).build();

        PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").accessors().type(SIMPLE)
                .transcoder(transcoder).invoker(invoker).build();

        entityMeta.setIdMeta(idMeta);

        long primaryKey = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getField())).thenReturn(name);
        when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(age);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(ageMeta, age)).thenReturn(age);

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForUpdate(context, ps, asList(nameMeta, ageMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, name, age, primaryKey);
    }

    @Test
    public void should_bind_for_simple_counter_increment_decrement() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta counterMeta = completeBean(Void.class, Long.class).field("count").transcoder(transcoder).invoker(invoker).build();

        Long primaryKey = RandomUtils.nextLong();
        Long counter = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(transcoder.forceEncodeToJSON(counter)).thenReturn(counter.toString());
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(ps.bind(counter, "CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForSimpleCounterIncrementDecrement(context, ps, counterMeta, counter, ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(counter, "CompleteBean", primaryKey.toString(), "count");
    }

    @Test
    public void should_bind_for_simple_counter_select() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta counterMeta = completeBean(Void.class, Long.class).field("count").transcoder(transcoder).invoker(invoker).build();

        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(ps.bind("CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForSimpleCounterSelect(context, ps, counterMeta, ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly("CompleteBean", primaryKey.toString(), "count");
    }

    @Test
    public void should_bind_for_simple_counter_delete() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta counterMeta = completeBean(Void.class, Long.class).field("count").transcoder(transcoder).invoker(invoker).build();

        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(ps.bind("CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForSimpleCounterDelete(context, ps, counterMeta);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly("CompleteBean", primaryKey.toString(), "count");
    }

    @Test
    public void should_bind_for_clustered_counter_increment_decrement() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        Long primaryKey = RandomUtils.nextLong();
        Long counter = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(ps.bind(0, counter, primaryKey)).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForClusteredCounterIncrementDecrement(context, ps, counter);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, counter, primaryKey);

    }

    @Test
    public void should_bind_for_clustered_counter_select() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(ps.bind(primaryKey)).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForClusteredCounterSelect(context, ps, ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey);

    }

    @Test
    public void should_bind_for_clustered_counter_delete() throws Exception {
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(ps.bind(primaryKey)).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForClusteredCounterDelete(context, ps);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);

        assertThat(asList(actual.getValues())).containsExactly(primaryKey);
    }

    @Test
    public void should_bind_for_remove_all_from_collection_and_map() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(changeSet.getChangeType()).thenReturn(REMOVE_COLLECTION_OR_MAP);
        when(ps.bind(0, null, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, null, primaryKey);
    }

    @Test
    public void should_bind_for_assign_value_to_set() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final Set<Object> values = Sets.<Object>newHashSet("whatever");
        when(changeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_SET);
        when(changeSet.getEncodedSetChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_assign_value_to_map() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final Map<Object, Object> values = ImmutableMap.<Object, Object>of(1, "whatever");
        when(changeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_MAP);
        when(changeSet.getEncodedMapChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_assign_value_to_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_LIST);
        when(changeSet.getEncodedListChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_add_element_to_set() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final Set<Object> values = Sets.<Object>newHashSet("whatever");
        when(changeSet.getChangeType()).thenReturn(ADD_TO_SET);
        when(changeSet.getEncodedSetChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_remove_element_from_set() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final Set<Object> values = Sets.<Object>newHashSet("whatever");
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_SET);
        when(changeSet.getEncodedSetChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_append_element_to_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(APPEND_TO_LIST);
        when(changeSet.getEncodedListChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_prepend_element_to_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(PREPEND_TO_LIST);
        when(changeSet.getEncodedListChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_remove_element_from_list() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST);
        when(changeSet.getEncodedListChanges()).thenReturn(values);
        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test(expected = IllegalStateException.class)
    public void should_bind_for_set_element_at_index_to_list() throws Exception {
        when(changeSet.getChangeType()).thenReturn(SET_TO_LIST_AT_INDEX);
        binder.bindForCollectionAndMapUpdate(context, ps, changeSet);
    }

    @Test(expected = IllegalStateException.class)
    public void should_bind_for_remove_element_at_index_to_list() throws Exception {
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST_AT_INDEX);
        binder.bindForCollectionAndMapUpdate(context, ps, changeSet);
    }

    @Test
    public void should_bind_for_add_elements_to_map_with_timestamp() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);
        Long primaryKey = RandomUtils.nextLong();

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.getTimestamp()).thenReturn(Optional.fromNullable(100L));

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final Map<Object, Object> values = ImmutableMap.<Object, Object>of(1, "whatever");
        when(changeSet.getChangeType()).thenReturn(ADD_TO_MAP);
        when(changeSet.getEncodedMapChanges()).thenReturn(values);
        when(ps.bind(0, 100L, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, 100L, values, primaryKey);
    }

    @Test
    public void should_bind_for_remove_entry_from_map_with_cas_condition() throws Exception {
        //Given
        PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID).invoker(invoker).build();
        Long primaryKey = RandomUtils.nextLong();
        final CASCondition CASCondition = new CASCondition("name", "John");

        EntityMeta meta = mock(EntityMeta.class);
        when(meta.getClassName()).thenReturn("CompleteBean");
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.getPrimaryKey(entity)).thenReturn(primaryKey);
        when(meta.encodeCasConditionValue(CASCondition)).thenReturn("John");

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.hasCasConditions()).thenReturn(true);
        when(context.getCasConditions()).thenReturn(asList(CASCondition));

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        final Map<Object, Object> values = ImmutableMap.<Object, Object>of(1, "whatever");
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_MAP);
        when(changeSet.getEncodedMapChanges()).thenReturn(values);
        when(ps.bind(0, 1, null, primaryKey, "John")).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, 1, null, primaryKey, "John");
    }
}
