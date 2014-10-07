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

import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.Update.Conditions;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;
import static info.archinnov.achilles.test.builders.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.Options.CASCondition;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.querybuilder.Update.Assignments;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Where;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;

@RunWith(MockitoJUnitRunner.class)
public class StatementGeneratorTest {

    @InjectMocks
    private StatementGenerator generator;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock
    private DaoContext daoContext;

    @Mock
    private PersistenceContext.StateHolderFacade context;

    @Mock
    private RegularStatementWrapper statementWrapper;

    @Mock
    private DirtyCheckChangeSet dirtyCheckChangeSet;

    @Captor
    private ArgumentCaptor<Conditions> conditionsCaptor;


    private ReflectionInvoker invoker = new ReflectionInvoker();

    @Test
    public void should_generate_set_element_at_index_to_list_with_cas_conditions() throws Exception {
        //Given

        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        Object[] boundValues = new Object[] { "whatever" };
        CompleteBean entity = builder().id(id).buid();
        final CASCondition casCondition = new CASCondition("name", "DuyHai");
        final Pair<Assignments, Object[]> updateClauseAndBoundValues = Pair.create(update(), boundValues);
        final Pair<Where, Object[]> whereClauseAndBoundValues = Pair.create(QueryBuilder.update("table").with(set("name", "DuyHai")).where(QueryBuilder.eq("id",11L)), boundValues);

        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getTtl()).thenReturn(Optional.fromNullable(10));
        when(context.getTimestamp()).thenReturn(Optional.fromNullable(100L));
        when(context.getCasConditions()).thenReturn(asList(casCondition));

        when(entityMeta.forTranscoding().encodeCasConditionValue(casCondition)).thenReturn("DuyHai_encoded");
        when(entityMeta.config().getKeyspaceName()).thenReturn("ks");
        when(entityMeta.config().getTableName()).thenReturn("table");

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(SET_TO_LIST_AT_INDEX);
        when(dirtyCheckChangeSet.generateUpdateForSetAtIndexElement(conditionsCaptor.capture())).thenReturn(updateClauseAndBoundValues);
        when(dirtyCheckChangeSet.getPropertyMeta()).thenReturn(idMeta);

        when(entityMeta.getIdMeta().forStatementGeneration().generateWhereClauseForUpdate(entity, idMeta, updateClauseAndBoundValues.left)).thenReturn(whereClauseAndBoundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(context, dirtyCheckChangeSet);

        //Then
        assertThat(conditionsCaptor.getValue().getQueryString()).isEqualTo("UPDATE ks.table USING TTL 10 AND TIMESTAMP 100 IF name=?;");
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table SET name=? WHERE id=11;");
        assertThat(pair.right[0]).isEqualTo(10);
        assertThat(pair.right[1]).isEqualTo(100L);
    }

    @Test
    public void should_generate_remove_element_at_index_to_list_update() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        Object[] boundValues = new Object[] { "whatever" };
        CompleteBean entity = builder().id(id).buid();
        final Pair<Assignments, Object[]> updateClauseAndBoundValues = Pair.create(update(), boundValues);
        final Pair<Where, Object[]> whereClauseAndBoundValues = Pair.create(QueryBuilder.update("table").with(set("name", "DuyHai")).where(QueryBuilder.eq("id",11L)), boundValues);

        when(context.getEntity()).thenReturn(entity);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(context.getIdMeta()).thenReturn(idMeta);
        when(context.getTtl()).thenReturn(Optional.fromNullable(10));
        when(context.getTimestamp()).thenReturn(Optional.fromNullable(100L));
        when(context.getCasConditions()).thenReturn(new ArrayList<CASCondition>());

        when(entityMeta.config().getQualifiedTableName()).thenReturn("table");

        when(dirtyCheckChangeSet.getChangeType()).thenReturn(REMOVE_FROM_LIST_AT_INDEX);
        when(dirtyCheckChangeSet.generateUpdateForRemovedAtIndexElement(any(Conditions.class))).thenReturn(updateClauseAndBoundValues);
        when(dirtyCheckChangeSet.getPropertyMeta()).thenReturn(idMeta);

        when(entityMeta.getIdMeta().forStatementGeneration().generateWhereClauseForUpdate(entity, idMeta, updateClauseAndBoundValues.left)).thenReturn(whereClauseAndBoundValues);

        //When
        final Pair<Where, Object[]> pair = generator.generateCollectionAndMapUpdateOperation(context, dirtyCheckChangeSet);

        //Then
        assertThat(pair.left.getQueryString()).isEqualTo("UPDATE table SET name=? WHERE id=11;");
        assertThat(pair.right[0]).isEqualTo(10);
    }

    private Update.Assignments update() {
        return QueryBuilder.update("table").with();
    }


}
