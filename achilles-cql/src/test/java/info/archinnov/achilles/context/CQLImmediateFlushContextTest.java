/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.context;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

@RunWith(MockitoJUnitRunner.class)
public class CQLImmediateFlushContextTest {

	private CQLImmediateFlushContext context;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private BoundStatementWrapper bsWrapper;

	@Mock
	private BoundStatement bs;

	@Mock
	private Statement statement;

	@Mock
	private Query query;

	@Before
	public void setUp() {
		context = new CQLImmediateFlushContext(daoContext, null);
		when(bsWrapper.getBs()).thenReturn(bs);
	}

	@Test
	public void should_return_IMMEDIATE_type() throws Exception {
		assertThat(context.type()).isSameAs(FlushType.IMMEDIATE);
	}

	@Test
	public void should_push_bound_statement_with_consistency() throws Exception {
		List<BoundStatementWrapper> boundStatementWrappers = new ArrayList<BoundStatementWrapper>();
		Whitebox.setInternalState(context, "boundStatementWrappers", boundStatementWrappers);

		context.pushBoundStatement(bsWrapper, EACH_QUORUM);

		verify(bs).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		assertThat(boundStatementWrappers).containsOnly(bsWrapper);
	}

	@Test
	public void should_push_bound_statement_with_consistency_overriden_by_current_level() throws Exception {
		List<BoundStatementWrapper> boundStatementWrappers = new ArrayList<BoundStatementWrapper>();
		Whitebox.setInternalState(context, "boundStatementWrappers", boundStatementWrappers);

		context.setConsistencyLevel(LOCAL_QUORUM);
		context.pushBoundStatement(bsWrapper, EACH_QUORUM);

		verify(bs).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
		assertThat(boundStatementWrappers).containsOnly(bsWrapper);
	}

	@Test
	public void should_push_statement_with_consistency() throws Exception {
		List<Statement> statements = new ArrayList<Statement>();
		Whitebox.setInternalState(context, "statements", statements);

		context.pushStatement(statement, EACH_QUORUM);

		verify(statement).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		assertThat(statements).containsOnly(statement);
	}

	@Test
	public void should_push_statement_with_consistency_overriden_by_current_level() throws Exception {
		List<Statement> statements = new ArrayList<Statement>();
		Whitebox.setInternalState(context, "statements", statements);

		context.setConsistencyLevel(LOCAL_QUORUM);
		context.pushStatement(statement, EACH_QUORUM);

		verify(statement).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
		assertThat(statements).containsOnly(statement);
	}

	@Test
	public void should_execute_immediate_with_consistency_level() throws Exception {
		ResultSet result = mock(ResultSet.class);
		when(daoContext.execute(query)).thenReturn(result);

		ResultSet actual = context.executeImmediateWithConsistency(query, EACH_QUORUM);

		assertThat(actual).isSameAs(result);
		verify(query).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_flush() throws Exception {
		List<BoundStatementWrapper> boundStatementWrappers = new ArrayList<BoundStatementWrapper>();
		boundStatementWrappers.add(bsWrapper);
		List<Statement> statements = new ArrayList<Statement>();
		statements.add(statement);

		Object[] boundValues = new Object[1];
		when(bsWrapper.getValues()).thenReturn(boundValues);
		Whitebox.setInternalState(context, "boundStatementWrappers", boundStatementWrappers);
		Whitebox.setInternalState(context, "statements", statements);

		context.flush();

		verify(daoContext).execute(bs, boundValues);
		verify(daoContext).execute(statement);
		assertThat(boundStatementWrappers).isEmpty();
		assertThat(statements).isEmpty();
	}

	@Test
	public void should_duplicate() throws Exception {
		context = new CQLImmediateFlushContext(daoContext, LOCAL_QUORUM);
		CQLImmediateFlushContext actual = context.duplicate();

		assertThat(actual.consistencyLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_calling_start_batch() throws Exception {
		context.startBatch();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_calling_end_batch() throws Exception {
		context.endBatch();
	}
}
