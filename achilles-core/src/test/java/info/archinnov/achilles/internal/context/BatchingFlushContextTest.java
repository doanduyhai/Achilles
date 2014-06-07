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
package info.archinnov.achilles.internal.context;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.type.Empty;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.EmptyFutureResultSets;
import info.archinnov.achilles.internal.context.AbstractFlushContext.FlushType;
import info.archinnov.achilles.internal.interceptor.EventHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BatchStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;

@RunWith(MockitoJUnitRunner.class)
public class BatchingFlushContextTest {

    private BatchingFlushContext context;

    @Mock
    private DaoContext daoContext;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private BoundStatementWrapper bsWrapper;

    @Mock
    private RegularStatement query;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet1;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet2;

    @Mock
    private ListenableFuture<List<ResultSet>> futureAsList;

    @Mock
    private ListenableFuture<Empty> futureEmpty;

    @Mock
    private AchillesFuture<Empty> achillesEmpty;


    @Captor
    private ArgumentCaptor<AbstractStatementWrapper> statementWrapperCaptor;

    @Captor
    private ArgumentCaptor<List<ListenableFuture<ResultSet>>> futureResultSetsCaptor;

    @Captor
    private ArgumentCaptor<Function<List<ResultSet>, Empty>> applyTriggersCaptor;

    private Optional<CASResultListener> NO_LISTENER = Optional.absent();
    private static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    @Before
    public void setUp() {
        context = new BatchingFlushContext(daoContext, EACH_QUORUM, NO_SERIAL_CONSISTENCY);
        context.asyncUtils = asyncUtils;
        when(daoContext.getExecutorService()).thenReturn(executorService);
    }

    @Test
    public void should_start_batch() throws Exception {
        context.startBatch();
    }

    @Test
    public void should_do_nothing_when_flush_is_called() throws Exception {
        context.statementWrappers.add(bsWrapper);

        final ListenableFuture<List<ResultSet>> actual = context.flush();

        assertThat(actual).isInstanceOf(EmptyFutureResultSets.class);
        assertThat(context.statementWrappers).containsExactly(bsWrapper);
    }

    @Test
    public void should_end_batch_with_logged_batch() throws Exception {
        //Given
        EventHolder eventHolder = mock(EventHolder.class);
        RegularStatement statement1 = QueryBuilder.select().from("table1");
        RegularStatement statement2 = QueryBuilder.select().from("table2");
        AbstractStatementWrapper wrapper1 = new RegularStatementWrapper(CompleteBean.class, statement1, null, com.datastax.driver.core.ConsistencyLevel.ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        AbstractStatementWrapper wrapper2 = new RegularStatementWrapper(CompleteBean.class, statement2, null, com.datastax.driver.core.ConsistencyLevel.ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        AbstractStatementWrapper wrapper3 = new RegularStatementWrapper(CompleteBean.class, statement2, null, com.datastax.driver.core.ConsistencyLevel.ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        context.eventHolders = asList(eventHolder);
        context.statementWrappers = asList(wrapper1, wrapper2);
        context.counterStatementWrappers = asList(wrapper3);
        context.consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        context.serialConsistencyLevel = Optional.fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);

        when(daoContext.execute(statementWrapperCaptor.capture())).thenReturn(futureResultSet1, futureResultSet2);
        when(asyncUtils.mergeResultSetFutures(futureResultSetsCaptor.capture())).thenReturn(futureAsList);
        when(asyncUtils.transformFuture(eq(futureAsList), applyTriggersCaptor.capture(), eq(executorService))).thenReturn(futureEmpty);
        when(asyncUtils.buildInterruptible(futureEmpty)).thenReturn(achillesEmpty);

        //When
        final ListenableFuture<Empty> futureResultSets = context.flushBatch();

        //Then
        assertThat(futureResultSets).isSameAs(achillesEmpty);
        assertThat(futureResultSetsCaptor.getValue()).containsExactly(futureResultSet1, futureResultSet2);

        final List<AbstractStatementWrapper> wrappers = statementWrapperCaptor.getAllValues();
        BatchStatementWrapper batchWrapper = (BatchStatementWrapper) wrappers.get(0);
        assertThat(Whitebox.<List<AbstractStatementWrapper>>getInternalState(batchWrapper, "statementWrappers")).containsExactly(wrapper1, wrapper2);
        assertThat(wrappers.get(1)).isSameAs(wrapper3);

        assertThat(batchWrapper.getStatement().getConsistencyLevel()).isSameAs(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
        assertThat(batchWrapper.getStatement().getSerialConsistencyLevel()).isNull();

        final Function<List<ResultSet>, Empty> applyTriggers = applyTriggersCaptor.getValue();
        applyTriggers.apply(asList(mock(ResultSet.class)));

        verify(eventHolder).triggerInterception();
    }

    @Test
    public void should_get_type() throws Exception {
        assertThat(context.type()).isSameAs(FlushType.BATCH);
    }

    @Test
    public void should_duplicate_without_ttl() throws Exception {
        context.statementWrappers.add(bsWrapper);

        BatchingFlushContext duplicate = context.duplicate();

        assertThat(duplicate.statementWrappers).containsOnly(bsWrapper);
        assertThat(duplicate.consistencyLevel).isSameAs(EACH_QUORUM);
        assertThat(duplicate.serialConsistencyLevel.isPresent()).isFalse();
    }

    @Test
    public void should_trigger_interceptor_immediately_for_POST_LOAD_event() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        Object entity = new Object();

        //When
        context.triggerInterceptor(meta, entity, Event.POST_LOAD);

        //Then
        verify(meta.forInterception()).intercept(entity, Event.POST_LOAD);
    }

    @Test
    public void should_push_interceptor_to_list() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        Object entity = new Object();

        //When
        context.triggerInterceptor(meta, entity, Event.POST_INSERT);

        //Then
        verify(meta.forInterception(), never()).intercept(entity, Event.POST_INSERT);
        assertThat(context.eventHolders).hasSize(1);
        final EventHolder eventHolder = context.eventHolders.get(0);
        eventHolder.triggerInterception();
        verify(meta.forInterception()).intercept(entity, Event.POST_INSERT);
    }

    @Test
    public void should_duplicate_with_no_data_but_consistency() throws Exception {
        //Given
        context.statementWrappers.add(mock(AbstractStatementWrapper.class));
        context.eventHolders.add(mock(EventHolder.class));

        //When
        final BatchingFlushContext newContext = context.duplicateWithNoData(ConsistencyLevel.EACH_QUORUM);

        //Then
        assertThat(newContext.statementWrappers).isEmpty();
        assertThat(newContext.eventHolders).isEmpty();
        assertThat(newContext.consistencyLevel).isEqualTo(ConsistencyLevel.EACH_QUORUM);
    }

    @Test
    public void should_duplicate_with_no_data_vut_consistency_and_serial_consistency() throws Exception {
        //Given
        context.statementWrappers.add(mock(AbstractStatementWrapper.class));
        context.eventHolders.add(mock(EventHolder.class));

        //When
        final BatchingFlushContext newContext = context.duplicateWithNoData(ConsistencyLevel.EACH_QUORUM,
                Optional.fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL));

        //Then
        assertThat(newContext.statementWrappers).isEmpty();
        assertThat(newContext.eventHolders).isEmpty();
        assertThat(newContext.consistencyLevel).isEqualTo(ConsistencyLevel.EACH_QUORUM);
        assertThat(newContext.serialConsistencyLevel.get()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
    }

}
