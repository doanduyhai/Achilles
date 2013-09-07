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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftBatchingFlushContextTest {
	private ThriftBatchingFlushContext context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao cfDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private ThriftConsistencyContext consistencyContext;

	@Mock
	private ThriftDaoContext thriftDaoContext;

	private Map<String, Pair<Mutator<?>, ThriftAbstractDao>> mutatorMap = new HashMap<String, Pair<Mutator<?>, ThriftAbstractDao>>();

	private Boolean hasCustomConsistencyLevels = false;

	@Before
	public void setUp() {
		context = new ThriftBatchingFlushContext(
				thriftDaoContext,
				consistencyContext,
				new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>(),
				hasCustomConsistencyLevels);

		Whitebox.setInternalState(context, ThriftConsistencyContext.class,
				consistencyContext);
		Whitebox.setInternalState(context, "mutatorMap", mutatorMap);
		Whitebox.setInternalState(context, ThriftDaoContext.class,
				thriftDaoContext);
		mutatorMap.clear();
	}

	@Test
	public void should_start_batch() throws Exception {
		context.startBatch();
		verify(consistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_do_nothing_when_flush_called() throws Exception {
		context.flush();
		verifyZeroInteractions(entityDao, consistencyContext);
	}

	@Test
	public void should_end_batch() throws Exception {
		Pair<Mutator<?>, ThriftAbstractDao> pair = Pair
				.<Mutator<?>, ThriftAbstractDao> create(mutator, entityDao);
		mutatorMap.put("cf", pair);

		context.endBatch();

		verify(entityDao).executeMutator(mutator);
		verify(consistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_get_type() throws Exception {
		assertThat(context.type()).isSameAs(FlushType.BATCH);
	}

	@Test
	public void should_duplicate_without_ttl() throws Exception {
		context = new ThriftBatchingFlushContext(
				thriftDaoContext,
				consistencyContext,
				new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>(),
				true);
		ThriftBatchingFlushContext actual = context.duplicate();

		assertThat(actual).isNotNull();
		assertThat(actual.consistencyLevel).isNull();
	}

}
