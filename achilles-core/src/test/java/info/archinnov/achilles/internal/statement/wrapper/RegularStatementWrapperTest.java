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

package info.archinnov.achilles.internal.statement.wrapper;

import static com.datastax.driver.core.ColumnDefinitionBuilder.buildColumnDef;
import static com.datastax.driver.core.ColumnDefinitions.Definition;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static info.archinnov.achilles.LogInterceptionRule.interceptDMLStatementViaMockedAppender;
import static info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper.LWT_RESULT_COLUMN;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation.INSERT;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation.UPDATE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import org.fest.assertions.data.MapEntry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.LogInterceptionRule.DMLStatementInterceptor;
import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.sample.entity.Entity1;

@RunWith(MockitoJUnitRunner.class)
public class RegularStatementWrapperTest {

    private CodecRegistry codecRegistry = new CodecRegistry();
    private RegularStatementWrapper wrapper;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private RegularStatement rs;

    @Mock
    private Session session;

    @Mock
    private ResultSetFuture resultSetFuture;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ResultSet resultSet;

    @Mock
    private Row row;


    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ColumnDefinitions columnDefinitions;

    @Mock
    private RowMethodInvoker invoker;

    @Captor
    private ArgumentCaptor<Statement> statementCaptor;

    private Optional<LWTResultListener> NO_LISTENER = Optional.absent();
    private static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    @Rule
    public DMLStatementInterceptor dmlStmntInterceptor = interceptDMLStatementViaMockedAppender();

    @Captor
    private ArgumentCaptor<LoggingEvent> loggingEvent;


    @Before
    public void setUp() {
        when(rs.getQueryString()).thenReturn("SELECT * FROM table");
    }

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, NO_LISTENER, Optional.of(ConsistencyLevel.LOCAL_SERIAL));
        wrapper.traceQueryForEntity = true;
        wrapper.asyncUtils = asyncUtils;
        when(session.executeAsync(statementCaptor.capture())).thenReturn(resultSetFuture);
        when(asyncUtils.applyLoggingTracingAndCASCheck(resultSetFuture, wrapper, executorService)).thenReturn(futureResultSet);

        //When
        final ListenableFuture<ResultSet> actual = wrapper.executeAsync(session, executorService);

        //Then
        assertThat(actual).isSameAs(futureResultSet);
        verify(rs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    }

    @Test
    public void should_execute_cas_successfully() throws Exception {
        //Given
        final AtomicBoolean LWTSuccess = new AtomicBoolean(false);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
                LWTSuccess.compareAndSet(false, true);
            }

            @Override
            public void onError(LWTResult lwtResult) {

            }
        };

        when(rs.getQueryString()).thenReturn("INSERT INTO table IF NOT EXISTS");
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, Optional.of(listener), NO_SERIAL_CONSISTENCY);
        when(resultSet.one().getBool(LWT_RESULT_COLUMN)).thenReturn(true);

        //When
        wrapper.checkForLWTSuccess(resultSet);

        //Then
        assertThat(LWTSuccess.get()).isTrue();
    }

    @Test
    public void should_throw_exception_on_cas_error() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicLWTResult = new AtomicReference<>(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicLWTResult.compareAndSet(null, lwtResult);
            }
        };
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, Optional.of(listener), NO_SERIAL_CONSISTENCY);
        wrapper.invoker = invoker;
        when(rs.getQueryString()).thenReturn("UPDATE table IF name='John' SET");
        when(resultSet.one()).thenReturn(row);
        when(row.getBool(LWT_RESULT_COLUMN)).thenReturn(false);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

        when(columnDefinitions.iterator().hasNext()).thenReturn(true, true, false);
        Definition col1 = buildColumnDef("keyspace", "table", "[applied]", DataType.cboolean());
        Definition col2 = buildColumnDef("keyspace", "table", "name", DataType.text());
        when(columnDefinitions.iterator().next()).thenReturn(col1, col2);

        when(invoker.invokeOnRowForType(row, codecRegistry.codecFor(DataType.cboolean()).getJavaType().getRawType(), "[applied]")).thenReturn(false);
        when(invoker.invokeOnRowForType(row, codecRegistry.codecFor(DataType.text()).getJavaType().getRawType(), "name")).thenReturn("Helen");

        //When
        wrapper.checkForLWTSuccess(resultSet);

        //Then
        final LWTResultListener.LWTResult actual = atomicLWTResult.get();
        assertThat(actual).isNotNull();
        assertThat(actual.operation()).isEqualTo(UPDATE);
        assertThat(actual.currentValues()).contains(MapEntry.entry("[applied]", false), MapEntry.entry("name", "Helen"));
    }

    @Test
    public void should_notify_listener_on_cas_error() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        wrapper.invoker = invoker;
        when(rs.getQueryString()).thenReturn("INSERT INTO table IF NOT EXISTS");
        when(resultSet.one()).thenReturn(row);
        when(row.getBool(LWT_RESULT_COLUMN)).thenReturn(false);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

        when(columnDefinitions.iterator().hasNext()).thenReturn(true, true, false);
        Definition col1 = buildColumnDef("keyspace", "table", "[applied]", DataType.cboolean());
        Definition col2 = buildColumnDef("keyspace", "table", "id", DataType.bigint());
        when(columnDefinitions.iterator().next()).thenReturn(col1, col2);

        when(invoker.invokeOnRowForType(row, codecRegistry.codecFor(DataType.cboolean()).getJavaType().getRawType(), "[applied]")).thenReturn(false);
        when(invoker.invokeOnRowForType(row, codecRegistry.codecFor(DataType.bigint()).getJavaType().getRawType(), "id")).thenReturn(10L);

        AchillesLightWeightTransactionException caughtEx = null;
        //When
        try {
            wrapper.checkForLWTSuccess(resultSet);
        } catch (AchillesLightWeightTransactionException ace) {
            caughtEx = ace;
        }

        //Then
        assertThat(caughtEx).isNotNull();
        assertThat(caughtEx.operation()).isEqualTo(INSERT);
        assertThat(caughtEx.currentValues()).contains(MapEntry.entry("[applied]", false), MapEntry.entry("id", 10L));
    }

    @Test
    public void should_get_statement() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);

        //When
        final RegularStatement expectedRs = wrapper.getStatement();

        //Then
        assertThat(expectedRs).isSameAs(rs);
    }

    @Test
    public void should_activate_query_tracing() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(Entity1.class, rs, new Object[] { 1 }, ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);

        //When
        wrapper.activateQueryTracing();

        //Then
        verify(rs).enableTracing();
    }

    @Test
    public void should_trace_query() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(Entity1.class, rs, new Object[] { 1 }, ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        wrapper.traceQueryForEntity = true;

        ExecutionInfo executionInfo = mock(ExecutionInfo.class, RETURNS_DEEP_STUBS);

        QueryTrace.Event event = mock(QueryTrace.Event.class);
        when(resultSet.getAllExecutionInfo()).thenReturn(asList(executionInfo));
        when(executionInfo.getAchievedConsistencyLevel()).thenReturn(ConsistencyLevel.ALL);
        when(executionInfo.getQueryTrace().getEvents()).thenReturn(asList(event));
        when(event.getDescription()).thenReturn("description");
        when(event.getSource()).thenReturn(InetAddress.getLocalHost());
        when(event.getSourceElapsedMicros()).thenReturn(100);
        when(event.getThreadName()).thenReturn("thread");

        //When
        wrapper.tracing(resultSet);

        //Then
    }

    @Test
    public void should_log_dml_of_a_regular_statement() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 73L, "bob" }, ONE, NO_LISTENER, Optional.<ConsistencyLevel>absent());
        when(rs.getQueryString()).thenReturn("SELECT * FROM table WHERE ...");
        when(rs.getConsistencyLevel()).thenReturn(ConsistencyLevel.LOCAL_QUORUM);

        // When
        wrapper.logDMLStatement("");

        // Then
        verify(dmlStmntInterceptor.appender(), times(2)).doAppend(loggingEvent.capture());
        final List<LoggingEvent> allValues = loggingEvent.getAllValues();
        final Object[] argumentArray1 = allValues.get(0).getArgumentArray();
        assertThat(argumentArray1[1]).isEqualTo("SELECT * FROM table WHERE ...");
        assertThat(argumentArray1[2]).isEqualTo("LOCAL_QUORUM");

        final Object[] argumentArray2 = allValues.get(1).getArgumentArray();
        final List<Object> boundValues = (List<Object>) argumentArray2[0];
        assertThat(boundValues.get(0)).isEqualTo(73L);
        assertThat(boundValues.get(1)).isEqualTo("bob");
    }
}
