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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Query;

@RunWith(MockitoJUnitRunner.class)
public class CQLBatchingFlushContextTest {

	private CQLBatchingFlushContext context;

	@Mock
	private CQLDaoContext daoContext;

	@Mock
	private BoundStatementWrapper bsWrapper;

	@Mock
	private Query query;

	@Before
	public void setUp() {
		context = new CQLBatchingFlushContext(daoContext, EACH_QUORUM);
	}

	@Test
	public void should_start_batch() throws Exception {
		context.boundStatementWrappers.add(bsWrapper);

		context.startBatch();

		assertThat(context.boundStatementWrappers).isEmpty();
		assertThat(context.consistencyLevel).isNull();
	}

	@Test
	public void should_do_nothing_when_flush_is_called() throws Exception {
		context.boundStatementWrappers.add(bsWrapper);

		context.flush();

		assertThat(context.boundStatementWrappers).containsExactly(bsWrapper);
	}

	@Test
	public void should_end_batch() throws Exception {
		context.boundStatementWrappers.add(bsWrapper);

		context.endBatch();

		assertThat(context.boundStatementWrappers).isEmpty();
		assertThat(context.consistencyLevel).isNull();
	}

	@Test
	public void should_get_type() throws Exception {
		assertThat(context.type()).isSameAs(FlushType.BATCH);
	}

	@Test
	public void should_duplicate_without_ttl() throws Exception {
		context.boundStatementWrappers.add(bsWrapper);

		CQLBatchingFlushContext duplicate = context.duplicate();

		assertThat(duplicate.boundStatementWrappers).containsOnly(bsWrapper);
		assertThat(duplicate.consistencyLevel).isSameAs(EACH_QUORUM);
	}
}
