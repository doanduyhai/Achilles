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

package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.persistence.operations.InternalCounterBuilder.*;
import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class InternalCounterBuilderTest {

	@Test
	public void should_build_counter_with_initial_value() throws Exception {
		assertThat(initialValue(10L).get()).isEqualTo(10L);
	}

	@Test
	public void should_build_counter_with_null_initial_value() throws Exception {
		assertThat(initialValue(null).get()).isNull();
	}

	@Test
	public void should_increment() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) incr();
		assertThat(counter.get()).isEqualTo(1L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(1L);
	}

	@Test
	public void should_decrement() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) decr();
		assertThat(counter.get()).isEqualTo(-1L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(-1L);
	}

	@Test
	public void should_increment_n() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) incr(5L);
		assertThat(counter.get()).isEqualTo(5L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(5L);
	}

	@Test
	public void should_decrement_n() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) decr(5L);
		assertThat(counter.get()).isEqualTo(-5L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(-5L);
	}

	@Test
	public void should_increment_with_initial_value() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) initialValue(10L);
		counter.incr();
		assertThat(counter.get()).isEqualTo(11L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(1L);
	}

	@Test
	public void should_decrement_with_initial_value() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) initialValue(10L);
		counter.decr();
		assertThat(counter.get()).isEqualTo(9L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(-1L);
	}

	@Test
	public void should_increment_n_with_initial_value() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) initialValue(10L);
		counter.incr(2L);
		assertThat(counter.get()).isEqualTo(12L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(2L);
	}

	@Test
	public void should_decrement_n_with_initial_value() throws Exception {
		InternalCounterImpl counter = (InternalCounterImpl) initialValue(10L);
		counter.decr(2L);
		assertThat(counter.get()).isEqualTo(8L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(-2L);
	}
}
