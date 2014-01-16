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

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class TypedMapTest {

	@Test
	public void should_get_typed() throws Exception {
		TypedMap map = new TypedMap();
		map.put("key", CounterBuilder.incr(10L));

		Counter counter = map.<Counter> getTyped("key");

		assertThat(counter).isNotNull();
		assertThat(counter.get()).isEqualTo(10L);
	}

	@Test
	public void should_get_typed_or_default() throws Exception {
		TypedMap map = new TypedMap();

		Counter counter = map.<Counter> getTypedOr("key", CounterBuilder.incr(10L));

		assertThat(counter).isNotNull();
		assertThat(counter.get()).isEqualTo(10L);
	}
}
