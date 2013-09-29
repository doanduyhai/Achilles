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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.type.Counter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CounterBuilderTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_incr() throws Exception {
		Counter counter = CounterBuilder.incr();
		assertThat(counter.get()).isEqualTo(1L);
	}

	@Test
	public void should_incr_n() throws Exception {
		Counter counter = CounterBuilder.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);
	}

	@Test
	public void should_decr() throws Exception {
		Counter counter = CounterBuilder.decr();
		assertThat(counter.get()).isEqualTo(-1L);
	}

	@Test
	public void should_decr_n() throws Exception {
		Counter counter = CounterBuilder.decr(10L);
		assertThat(counter.get()).isEqualTo(-10L);
	}

	@Test
	public void should_exception_when_calling_get_with_consistency() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.get(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_incr() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.incr();
	}

	@Test
	public void should_exception_when_calling_incr_n() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.incr(10L);
	}

	@Test
	public void should_exception_when_calling_decr() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.decr();
	}

	@Test
	public void should_exception_when_calling_decr_n() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.decr(10L);
	}

	@Test
	public void should_exception_when_calling_incr_with_consistency() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.incr(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_incr_n_with_consistency() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.incr(10L, EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_decr_with_consistency() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.decr(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_decr_n_with_consistency() throws Exception {
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not meant to be called");
		counter.decr(10L, EACH_QUORUM);
	}
}
