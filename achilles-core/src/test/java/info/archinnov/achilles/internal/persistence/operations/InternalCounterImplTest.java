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

package info.archinnov.achilles.internal.persistence.operations;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class InternalCounterImplTest {

	@Test
	public void should_get_value_from_initial_value() throws Exception {
		assertThat(new InternalCounterImpl(null, 10L).get()).isEqualTo(10L);
	}

	@Test
	public void should_get_value_from_initial_and_delta_values() throws Exception {
		assertThat(new InternalCounterImpl(1L, 10L).get()).isEqualTo(11L);
	}

	@Test
	public void should_get_value_from_delta_value_only() throws Exception {
		assertThat(new InternalCounterImpl(2L, null).get()).isEqualTo(2L);
	}

	@Test
	public void should_get_null_value_from_delta_value_only() throws Exception {
		assertThat(new InternalCounterImpl(null, null).get()).isNull();
	}

	@Test
	public void should_increment_from_null_delta() throws Exception {
		InternalCounterImpl counterImpl = new InternalCounterImpl(null, null);
		counterImpl.incr();

		assertThat(counterImpl.get()).isEqualTo(1L);
	}

	@Test
	public void should_decrement_from_null_delta() throws Exception {
		InternalCounterImpl counterImpl = new InternalCounterImpl(null, null);
		counterImpl.decr();

		assertThat(counterImpl.get()).isEqualTo(-1L);
	}

	@Test
	public void should_increment_n_from_null_delta() throws Exception {
		InternalCounterImpl counterImpl = new InternalCounterImpl(null, null);
		counterImpl.incr(2L);

		assertThat(counterImpl.get()).isEqualTo(2L);
	}

	@Test
	public void should_decrement_n_from_null_delta() throws Exception {
		InternalCounterImpl counterImpl = new InternalCounterImpl(null, null);
		counterImpl.decr(2L);

		assertThat(counterImpl.get()).isEqualTo(-2L);
	}
}
