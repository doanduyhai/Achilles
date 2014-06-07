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

import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.AbstractFlushContext.FlushType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class ImmediateFlushContextTest {

	private  static final Optional<ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    private ImmediateFlushContext context;

    @Mock
    private DaoContext daoContext;

    @Mock
    private BoundStatementWrapper bsWrapper;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private Statement statement;

    @Mock
    private RegularStatement query;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet1;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet2;

    @Mock
    private ListenableFuture<List<ResultSet>> futureAsList;

    @Captor
    private ArgumentCaptor<AbstractStatementWrapper> statementWrapperCaptor;

    @Captor
    private ArgumentCaptor<List<ListenableFuture<ResultSet>>> futureResultSetsCaptor;

    private Optional<CASResultListener> NO_LISTENER = Optional.absent();

    @Before
    public void setUp() {
        context = new ImmediateFlushContext(daoContext, null, NO_SERIAL_CONSISTENCY);
        context.asyncUtils = asyncUtils;
    }

	@Test
	public void should_duplicate() throws Exception {
		context = new ImmediateFlushContext(daoContext, LOCAL_QUORUM,
                Optional.fromNullable(ConsistencyLevel.LOCAL_SERIAL));
		ImmediateFlushContext actual = context.duplicate();

		assertThat(actual.consistencyLevel).isEqualTo(LOCAL_QUORUM);
		assertThat(actual.serialConsistencyLevel.get()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);
	}

    @Test
    public void should_return_IMMEDIATE_type() throws Exception {
        assertThat(context.type()).isSameAs(FlushType.IMMEDIATE);
    }

    @Test
    public void should_push_statement() throws Exception {
        List<AbstractStatementWrapper> statementWrappers = new ArrayList<>();
        Whitebox.setInternalState(context, "statementWrappers", statementWrappers);

        context.pushStatement(bsWrapper);
        assertThat(statementWrappers).containsOnly(bsWrapper);
    }

    @Test
    public void should_execute_immediate_with_consistency_level() throws Exception {
        when(daoContext.execute(bsWrapper)).thenReturn(futureResultSet1);

        ListenableFuture<ResultSet> actual = context.execute(bsWrapper);

        assertThat(actual).isSameAs(futureResultSet1);
    }

    @Test
    public void should_flush() throws Exception {
        // Given
        RegularStatement statement1 = QueryBuilder.select().from("table1");
        RegularStatement statement2 = QueryBuilder.select().from("table2");
        AbstractStatementWrapper wrapper1 = new RegularStatementWrapper(CompleteBean.class, statement1, null, com.datastax.driver.core.ConsistencyLevel.ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        AbstractStatementWrapper wrapper2 = new RegularStatementWrapper(CompleteBean.class, statement2, null, com.datastax.driver.core.ConsistencyLevel.ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);
        context.statementWrappers = asList(wrapper1);
        context.counterStatementWrappers = asList(wrapper2);

        when(daoContext.execute(statementWrapperCaptor.capture())).thenReturn(futureResultSet1, futureResultSet2);
        when(asyncUtils.mergeResultSetFutures(futureResultSetsCaptor.capture())).thenReturn(futureAsList);


        // When
        final ListenableFuture<List<ResultSet>> futureResultSets = context.flush();

        // Then
        assertThat(futureResultSets).isSameAs(futureAsList);
        assertThat(futureResultSetsCaptor.getValue()).containsExactly(futureResultSet1, futureResultSet2);
    }


    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_calling_start_batch() throws Exception {
        context.startBatch();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_calling_end_batch() throws Exception {
        context.flushBatch();
    }


    @Test
    public void should_trigger_interceptor() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        Object entity = new Object();

        //When
        context.triggerInterceptor(meta, entity, Event.POST_INSERT);

        //Then
        verify(meta.forInterception()).intercept(entity, Event.POST_INSERT);

    }
}
