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
package info.archinnov.achilles.type;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CounterBuilderTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_incr() throws Exception {
		CounterImpl counter = (CounterImpl)CounterBuilder.incr();
		assertThat(counter.getInternalCounterDelta()).isEqualTo(1L);
	}

	@Test
	public void should_incr_n() throws Exception {
        CounterImpl counter = (CounterImpl)CounterBuilder.incr(10L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(10L);
	}

	@Test
	public void should_decr() throws Exception {
        CounterImpl counter = (CounterImpl)CounterBuilder.decr();
		assertThat(counter.getInternalCounterDelta()).isEqualTo(-1L);
	}

	@Test
	public void should_decr_n() throws Exception {
        CounterImpl counter = (CounterImpl)CounterBuilder.decr(10L);
		assertThat(counter.getInternalCounterDelta()).isEqualTo(-10L);
	}





	@Test
	public void should_be_able_to_serialize_and_deserialize_counter_impl() throws Exception {

		ObjectMapper mapper = new ObjectMapper();
		Counter counter = CounterBuilder.incr(11L);

		String serialized = mapper.writeValueAsString(counter);
		assertThat(serialized).isEqualTo("\"11\"");

		CounterImpl deserialized = (CounterImpl)mapper.readValue(serialized, Counter.class);
		assertThat(deserialized.getInternalCounterDelta()).isEqualTo(11L);

		assertThat(mapper.writeValueAsString(CounterBuilder.incr(0))).isEqualTo("\"0\"");
	}
}
