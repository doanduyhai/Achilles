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
package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;

@RunWith(MockitoJUnitRunner.class)
public class CQLCounterWrapperTest {

	private CQLCounterWrapper wrapper;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PersistenceContext context;

	@Mock
	private PropertyMeta counterMeta;

	@Before
	public void setUp() {
		when(counterMeta.getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);
		when(counterMeta.getWriteConsistencyLevel()).thenReturn(EACH_QUORUM);

		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(ONE));

	}

	@Test
	public void should_get_simple_counter_with_default_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(context.getSimpleCounter(counterMeta, LOCAL_QUORUM)).thenReturn(counterValue);

		assertThat(wrapper.get()).isEqualTo(counterValue);
	}

	@Test
	public void should_get_simple_counter_with_runtime_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		when(context.getSimpleCounter(counterMeta, ONE)).thenReturn(counterValue);

		assertThat(wrapper.get()).isEqualTo(counterValue);
	}

	@Test
	public void should_get_simple_counter_with_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		when(context.getSimpleCounter(counterMeta, THREE)).thenReturn(counterValue);

		assertThat(wrapper.get(THREE)).isEqualTo(counterValue);
	}

	@Test
	public void should_get_clustered_counter() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		when(context.getClusteredCounter(counterMeta, ONE)).thenReturn(counterValue);

		assertThat(wrapper.get()).isEqualTo(counterValue);
	}

	@Test
	public void should_get_clustered_counter_with_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		when(context.getClusteredCounter(counterMeta, TWO)).thenReturn(counterValue);

		assertThat(wrapper.get(TWO)).isEqualTo(counterValue);
	}

	@Test
	public void should_increment_simple_counter() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr();
		verify(context).incrementSimpleCounter(counterMeta, 1L, ONE);
	}

	@Test
	public void should_increment_clustered_counter() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr();
		verify(context).incrementClusteredCounter(1L, ONE);
	}

	@Test
	public void should_increment_simple_counter_with_consistency() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr(EACH_QUORUM);
		verify(context).incrementSimpleCounter(counterMeta, 1L, EACH_QUORUM);
	}

	@Test
	public void should_increment_clustered_counter_with_consistency() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr(EACH_QUORUM);
		verify(context).incrementClusteredCounter(1L, EACH_QUORUM);
	}

	@Test
	public void should_increment_n_simple_counter() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr(counterValue);
		verify(context).incrementSimpleCounter(counterMeta, counterValue, ONE);
	}

	@Test
	public void should_increment_n_clustered_counter() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr(counterValue);
		verify(context).incrementClusteredCounter(counterValue, ONE);
	}

	@Test
	public void should_increment_n_simple_counter_with_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr(counterValue, EACH_QUORUM);
		verify(context).incrementSimpleCounter(counterMeta, counterValue, EACH_QUORUM);
	}

	@Test
	public void should_increment_n_clustered_counter_with_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.incr(counterValue, EACH_QUORUM);
		verify(context).incrementClusteredCounter(counterValue, EACH_QUORUM);
	}

	@Test
	public void should_decrement_simple_counter() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr();
		verify(context).decrementSimpleCounter(counterMeta, 1L, ONE);
	}

	@Test
	public void should_decrement_clustered_counter() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr();
		verify(context).decrementClusteredCounter(1L, ONE);
	}

	@Test
	public void should_decrement_simple_counter_with_consistency() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr(EACH_QUORUM);
		verify(context).decrementSimpleCounter(counterMeta, 1L, EACH_QUORUM);
	}

	@Test
	public void should_decrement_clustered_counter_with_consistency() throws Exception {
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr(EACH_QUORUM);
		verify(context).decrementClusteredCounter(1L, EACH_QUORUM);
	}

	@Test
	public void should_decrement_n_simple_counter() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr(counterValue);
		verify(context).decrementSimpleCounter(counterMeta, counterValue, ONE);
	}

	@Test
	public void should_decrement_n_clustered_counter() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr(counterValue);
		verify(context).decrementClusteredCounter(counterValue, ONE);
	}

	@Test
	public void should_decrement_n_simple_counter_with_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr(counterValue, EACH_QUORUM);
		verify(context).decrementSimpleCounter(counterMeta, counterValue, EACH_QUORUM);
	}

	@Test
	public void should_decrement_n_clustered_counter_with_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();
		when(context.getEntityMeta().isClusteredCounter()).thenReturn(true);
		wrapper = new CQLCounterWrapper(context, counterMeta);

		wrapper.decr(counterValue, EACH_QUORUM);
		verify(context).decrementClusteredCounter(counterValue, EACH_QUORUM);
	}
}
