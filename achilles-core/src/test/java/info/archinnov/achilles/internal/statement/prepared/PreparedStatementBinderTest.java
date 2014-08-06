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

import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
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
import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.Options.CASCondition;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
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
import org.mockito.Answers;
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
    private  static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private PreparedStatement ps;

    @Mock
    private BoundStatement bs;

    @Mock
    private ObjectMapper objectMapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DirtyCheckChangeSet changeSet;

    @Mock
    private PersistenceContext.StateHolderFacade context;

    @Mock
    private ConsistencyOverrider overrider;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
    public void setUp() {
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(context.getTtl()).thenReturn(NO_TTL);
        when(context.getTimestamp()).thenReturn(NO_TIMESTAMP);
        when(context.getConsistencyLevel()).thenReturn(NO_CONSISTENCY);
        when(context.getSerialConsistencyLevel()).thenReturn(NO_SERIAL_CONSISTENCY);
    }

    @Test
    public void should_bind_for_insert_with_simple_id() throws Exception {
        long primaryKey = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta ageMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);
        when(nameMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(name);
        when(ageMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(age);

        when(context.getSerialConsistencyLevel()).thenReturn(fromNullable(ConsistencyLevel.LOCAL_SERIAL));

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForInsert(context, ps, asList(nameMeta, ageMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        verify(bs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey, name, age, 0);
    }

    @Test
    public void should_bind_for_insert_with_null_fields() throws Exception {
        long primaryKey = RandomUtils.nextLong();
        String name = "name";

        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta ageMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);
        when(nameMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(name);
        when(ageMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(null);

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForInsert(context, ps, asList(nameMeta, ageMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey, name, null, 0);
    }

    @Test
    public void should_bind_for_insert_with_compound_key() throws Exception {
        long userId = RandomUtils.nextLong();
        String name = "name";
        String address = "30 WallStreet";
        int age = 30;
        EmbeddedKey primaryKey = new EmbeddedKey(userId, name);

        PropertyMeta addressMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta ageMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(true);
        when(idMeta.forTranscoding().encodeToComponents(primaryKey, false)).thenReturn(Arrays.<Object>asList(userId, name));
        when(addressMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(address);
        when(ageMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(age);

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForInsert(context, ps, asList(addressMeta, ageMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(userId, name, address, age, 0);
    }

    @Test
    public void should_bind_with_only_pk_in_where_clause() throws Exception {
        long userId = RandomUtils.nextLong();
        String name = "name";
        EmbeddedKey primaryKey = new EmbeddedKey(userId, name);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(true);
        when(idMeta.forTranscoding().encodeToComponents(primaryKey, true)).thenReturn(Arrays.<Object>asList(userId, name));

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindStatementWithOnlyPKInWhereClause(context, ps, true, info.archinnov.achilles.type.ConsistencyLevel.ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(userId, name);
    }

    @Test
    public void should_bind_for_update() throws Exception {
        long primaryKey = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta ageMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(context.getSerialConsistencyLevel()).thenReturn(fromNullable(ConsistencyLevel.LOCAL_SERIAL));

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);
        when(nameMeta.structure().isStaticColumn()).thenReturn(false);
        when(ageMeta.structure().isStaticColumn()).thenReturn(false);
        when(nameMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(name);
        when(ageMeta.forTranscoding().getAndEncodeValueForCassandra(entity)).thenReturn(age);

        when(ps.bind(Matchers.anyVararg())).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForUpdate(context, ps, asList(nameMeta, ageMeta));

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        verify(bs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        assertThat(asList(actual.getValues())).containsExactly(0, name, age, primaryKey);
    }

    @Test
    public void should_bind_for_simple_counter_increment_decrement() throws Exception {
        Long primaryKey = RandomUtils.nextLong();
        Long increment = RandomUtils.nextLong();
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(entityMeta.getClassName()).thenReturn("CompleteBean");
        when(idMeta.forTranscoding().forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(counterMeta.getCQL3ColumnName()).thenReturn("count");

        when(ps.bind(increment, "CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForSimpleCounterIncrementDecrement(context, ps, counterMeta, increment, ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(increment, "CompleteBean", primaryKey.toString(), "count");
    }

    @Test
    public void should_bind_for_simple_counter_select() throws Exception {

        Long primaryKey = RandomUtils.nextLong();
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(entityMeta.getClassName()).thenReturn("CompleteBean");
        when(idMeta.forTranscoding().forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(counterMeta.getCQL3ColumnName()).thenReturn("count");

        when(ps.bind("CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForSimpleCounterSelect(context, ps, counterMeta, ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly("CompleteBean", primaryKey.toString(), "count");
    }

    @Test
    public void should_bind_for_simple_counter_delete() throws Exception {
        Long primaryKey = RandomUtils.nextLong();
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(entityMeta.getClassName()).thenReturn("CompleteBean");
        when(idMeta.forTranscoding().forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(counterMeta.getCQL3ColumnName()).thenReturn("count");

        when(ps.bind("CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForSimpleCounterDelete(context, ps, counterMeta);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly("CompleteBean", primaryKey.toString(), "count");
    }

    @Test
    public void should_bind_for_clustered_counter_increment_decrement() throws Exception {
        Long primaryKey = RandomUtils.nextLong();
        Long increment = RandomUtils.nextLong();
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        when(counterMeta.structure().isStaticColumn()).thenReturn(false);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);


        when(ps.bind(increment, primaryKey)).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForClusteredCounterIncrementDecrement(context, ps, counterMeta, increment);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(increment, primaryKey);
    }

    @Test
    public void should_bind_for_clustered_counter_select() throws Exception {
        Long primaryKey = RandomUtils.nextLong();
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        when(counterMeta.structure().isStaticColumn()).thenReturn(false);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

        when(ps.bind(primaryKey)).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForClusteredCounterSelect(context, ps, true,ALL);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(primaryKey);
    }

    @Test
    public void should_bind_for_clustered_counter_delete() throws Exception {
        Long primaryKey = RandomUtils.nextLong();
        PropertyMeta counterMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(counterMeta.structure().isStaticColumn()).thenReturn(false);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

        when(ps.bind(primaryKey)).thenReturn(bs);

        BoundStatementWrapper actual = binder.bindForClusteredCounterDelete(context, ps);

        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);

        assertThat(asList(actual.getValues())).containsExactly(primaryKey);
    }

    @Test
    public void should_bind_for_remove_all_from_collection_and_map() throws Exception {
        //Given
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);

        when(overrider.getWriteLevel(context)).thenReturn(ALL);
        when(changeSet.getChangeType()).thenReturn(REMOVE_COLLECTION_OR_MAP);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final Set<Object> values = Sets.<Object>newHashSet("whatever");
        when(changeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_SET);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedSetChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final Map<Object, Object> values = ImmutableMap.<Object, Object>of(1, "whatever");
        when(changeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_MAP);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedMapChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(ASSIGN_VALUE_TO_LIST);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedListChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.getSerialConsistencyLevel()).thenReturn(fromNullable(ConsistencyLevel.LOCAL_SERIAL));
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final Set<Object> values = Sets.<Object>newHashSet("whatever");
        when(changeSet.getChangeType()).thenReturn(ADD_TO_SET);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedSetChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

        when(ps.bind(0, values, primaryKey)).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        verify(bs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        assertThat(asList(actual.getValues())).containsExactly(0, values, primaryKey);
    }

    @Test
    public void should_bind_for_remove_element_from_set() throws Exception {
        //Given
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final Set<Object> values = Sets.<Object>newHashSet("whatever");
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_SET);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedSetChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(APPEND_TO_LIST);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedListChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(PREPEND_TO_LIST);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedListChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final List<Object> values = Arrays.<Object>asList("whatever");
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedListChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();

        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.getTimestamp()).thenReturn(fromNullable(100L));
        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final Map<Object, Object> values = ImmutableMap.<Object, Object>of(1, "whatever");
        when(changeSet.getChangeType()).thenReturn(ADD_TO_MAP);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedMapChanges()).thenReturn(values);

        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

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
        Long primaryKey = RandomUtils.nextLong();
        final CASCondition CASCondition = new CASCondition("name", "John");


        when(context.getPrimaryKey()).thenReturn(primaryKey);
        when(context.hasCasConditions()).thenReturn(true);
        when(context.getCasConditions()).thenReturn(asList(CASCondition));

        when(overrider.getWriteLevel(context)).thenReturn(ALL);

        final Map<Object, Object> values = ImmutableMap.<Object, Object>of(1, "whatever");
        when(changeSet.getChangeType()).thenReturn(REMOVE_FROM_MAP);
        when(changeSet.getPropertyMeta().structure().isStaticColumn()).thenReturn(false);
        when(changeSet.getEncodedMapChanges()).thenReturn(values);

        when(entityMeta.getClassName()).thenReturn("CompleteBean");
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(entityMeta.forTranscoding().encodeCasConditionValue(CASCondition)).thenReturn("John");
        when(entityMeta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);
        when(idMeta.structure().isEmbeddedId()).thenReturn(false);
        when(idMeta.forTranscoding().encodeToCassandra(primaryKey)).thenReturn(primaryKey);

        when(ps.bind(0, 1, null, primaryKey, "John")).thenReturn(bs);

        //When
        final BoundStatementWrapper actual = binder.bindForCollectionAndMapUpdate(context, ps, changeSet);

        //Then
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        assertThat(asList(actual.getValues())).containsExactly(0, 1, null, primaryKey, "John");
    }
}
