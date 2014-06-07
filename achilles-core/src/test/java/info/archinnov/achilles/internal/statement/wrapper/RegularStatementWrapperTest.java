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
import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper.CAS_RESULT_COLUMN;
import static info.archinnov.achilles.listener.CASResultListener.CASResult;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation.INSERT;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation.UPDATE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import org.fest.assertions.data.MapEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.reflection.RowMethodInvoker;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.sample.entity.Entity1;

@RunWith(MockitoJUnitRunner.class)
public class RegularStatementWrapperTest {

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

    private Optional<CASResultListener> NO_LISTENER = Optional.absent();
    private static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    @Before
    public void setUp() {
        when(rs.getQueryString()).thenReturn("SELECT * FROM table");
    }

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, NO_LISTENER, fromNullable(ConsistencyLevel.LOCAL_SERIAL));
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
        final AtomicBoolean casSuccess = new AtomicBoolean(false);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
                casSuccess.compareAndSet(false, true);
            }

            @Override
            public void onCASError(CASResult casResult) {

            }
        };

        when(rs.getQueryString()).thenReturn("INSERT INTO table IF NOT EXISTS");
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, fromNullable(listener), NO_SERIAL_CONSISTENCY);
        when(resultSet.one().getBool(CAS_RESULT_COLUMN)).thenReturn(true);

        //When
        wrapper.checkForCASSuccess(resultSet);

        //Then
        assertThat(casSuccess.get()).isTrue();
    }

    @Test
    public void should_throw_exception_on_cas_error() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference<>(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };
        wrapper = new RegularStatementWrapper(CompleteBean.class, rs, new Object[] { 1 }, ONE, fromNullable(listener), NO_SERIAL_CONSISTENCY);
        wrapper.invoker = invoker;
        when(rs.getQueryString()).thenReturn("UPDATE table IF name='John' SET");
        when(resultSet.one()).thenReturn(row);
        when(row.getBool(CAS_RESULT_COLUMN)).thenReturn(false);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

        when(columnDefinitions.iterator().hasNext()).thenReturn(true, true, false);
        Definition col1 = buildColumnDef("keyspace", "table", "[applied]", DataType.cboolean());
        Definition col2 = buildColumnDef("keyspace", "table", "name", DataType.text());
        when(columnDefinitions.iterator().next()).thenReturn(col1, col2);

        when(invoker.invokeOnRowForType(row, DataType.cboolean().asJavaClass(), "[applied]")).thenReturn(false);
        when(invoker.invokeOnRowForType(row, DataType.text().asJavaClass(), "name")).thenReturn("Helen");

        //When
        wrapper.checkForCASSuccess(resultSet);

        //Then
        final CASResult actual = atomicCASResult.get();
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
        when(row.getBool(CAS_RESULT_COLUMN)).thenReturn(false);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);

        when(columnDefinitions.iterator().hasNext()).thenReturn(true, true, false);
        Definition col1 = buildColumnDef("keyspace", "table", "[applied]", DataType.cboolean());
        Definition col2 = buildColumnDef("keyspace", "table", "id", DataType.bigint());
        when(columnDefinitions.iterator().next()).thenReturn(col1, col2);

        when(invoker.invokeOnRowForType(row, DataType.cboolean().asJavaClass(), "[applied]")).thenReturn(false);
        when(invoker.invokeOnRowForType(row, DataType.bigint().asJavaClass(), "id")).thenReturn(10L);

        AchillesLightWeightTransactionException caughtEx = null;
        //When
        try {
            wrapper.checkForCASSuccess(resultSet);
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
}
