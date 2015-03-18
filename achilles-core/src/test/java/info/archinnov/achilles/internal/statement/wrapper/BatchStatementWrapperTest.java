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

import static com.datastax.driver.core.BatchStatement.Type.LOGGED;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.listener.LWTResultListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.type.ConsistencyLevel;


@RunWith(MockitoJUnitRunner.class)
public class BatchStatementWrapperTest {

    @Mock
    private AbstractStatementWrapper statementWrapper;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Session session;

    @Mock
    private ResultSetFuture resultSetFuture;

    @Mock
    private LWTResultListener LWTResultListener;

    @Mock
    private info.archinnov.achilles.listener.LWTResultListener.LWTResult LWTResult;

    @Captor
    private ArgumentCaptor<BatchStatement> batchCaptor;

    @Test
    public void should_get_query_string() throws Exception {
        //Given
        when(statementWrapper.getQueryString()).thenReturn("SELECT * FROM");
        statementWrapper.lwtResultListener = Optional.of(LWTResultListener);

        //When
        BatchStatementWrapper wrapper = new BatchStatementWrapper(LOGGED, asList(statementWrapper),
                Optional.of(ConsistencyLevel.ALL),Optional.of(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL));
        final String actual = wrapper.getQueryString();

        //Then
        assertThat(actual).isEqualTo("SELECT * FROM");
        verify(statementWrapper).activateQueryTracing();
        assertThat(wrapper.getStatement().getConsistencyLevel()).isEqualTo(com.datastax.driver.core.ConsistencyLevel.ALL);
        assertThat(wrapper.getStatement().getSerialConsistencyLevel()).isNull();
        assertThat(wrapper.lwtResultListener.get()).isInstanceOf(BatchStatementWrapper.CompositeLWTResultListener.class);

        wrapper.lwtResultListener.get().onSuccess();
        verify(LWTResultListener).onSuccess();

        wrapper.lwtResultListener.get().onError(LWTResult);
        verify(LWTResultListener).onError(LWTResult);
    }

    @Test
    public void should_log_dml_statements() throws Exception {
        //Given
        when(statementWrapper.isTracingEnabled()).thenReturn(true);
        statementWrapper.lwtResultListener = Optional.of(LWTResultListener);

        //When
        BatchStatementWrapper wrapper = new BatchStatementWrapper(LOGGED, asList(statementWrapper),
                Optional.<ConsistencyLevel>absent(), Optional.<com.datastax.driver.core.ConsistencyLevel>absent());
        wrapper.logDMLStatement("aa");

        //Then
        verify(statementWrapper).activateQueryTracing();
        verify(statementWrapper).logDMLStatement("aa");
        assertThat(wrapper.lwtResultListener.get()).isInstanceOf(BatchStatementWrapper.CompositeLWTResultListener.class);
    }

}
