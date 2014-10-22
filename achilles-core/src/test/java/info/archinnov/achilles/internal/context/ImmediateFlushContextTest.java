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
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.AbstractFlushContext.FlushType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;

@RunWith(MockitoJUnitRunner.class)
public class ImmediateFlushContextTest {

    private  static final Optional<ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

	private ImmediateFlushContext context;

	@Mock
	private DaoContext daoContext;

	@Mock
	private BoundStatementWrapper bsWrapper;

	@Mock
	private Statement statement;

	@Mock
	private RegularStatement query;
	@Before
	public void setUp() {
		context = new ImmediateFlushContext(daoContext, null, NO_SERIAL_CONSISTENCY);
	}

	@Test
	public void should_return_IMMEDIATE_type() throws Exception {
		assertThat(context.type()).isSameAs(FlushType.IMMEDIATE);
	}

	@Test
	public void should_push_statement() throws Exception {
		List<AbstractStatementWrapper> statementWrappers = new ArrayList<AbstractStatementWrapper>();
		Whitebox.setInternalState(context, "statementWrappers", statementWrappers);

		context.pushStatement(bsWrapper);
		assertThat(statementWrappers).containsOnly(bsWrapper);
	}

	@Test
	public void should_execute_immediate_with_consistency_level() throws Exception {
		ResultSet result = mock(ResultSet.class);
		when(daoContext.execute(bsWrapper)).thenReturn(result);

		ResultSet actual = context.executeImmediate(bsWrapper);

		assertThat(actual).isSameAs(result);
	}

	@Test
	public void should_flush() throws Exception {
		List<AbstractStatementWrapper> statementWrappers = new ArrayList<AbstractStatementWrapper>();
		statementWrappers.add(bsWrapper);
		Whitebox.setInternalState(context, "statementWrappers", statementWrappers);

		context.flush();

		verify(daoContext).execute(bsWrapper);
	}

	@Test
	public void should_duplicate() throws Exception {
		context = new ImmediateFlushContext(daoContext, LOCAL_QUORUM,
                Optional.fromNullable(ConsistencyLevel.LOCAL_SERIAL));
		ImmediateFlushContext actual = context.duplicate();

		assertThat(actual.consistencyLevel).isEqualTo(LOCAL_QUORUM);
		assertThat(actual.serialConsistencyLevel.get()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_calling_start_batch() throws Exception {
		context.startBatch();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_calling_end_batch() throws Exception {
		context.endBatch();
	}


    @Test
    public void should_trigger_interceptor() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        Object entity = new Object();

        //When
        context.triggerInterceptor(meta,entity, Event.POST_INSERT);

        //Then
        verify(meta.forInterception()).intercept(entity,Event.POST_INSERT);

    }
}
