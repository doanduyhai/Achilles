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

package info.archinnov.achilles.options;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;
import info.archinnov.achilles.type.TypedMap;
import org.junit.Test;

public class TypedMapTest {

	@Test
	public void should_get_typed_when_present() throws Exception {
		TypedMap map = new TypedMap();
		map.put("key", CounterBuilder.incr(10L));

		Counter counter = map.<Counter> getTyped("key");

		assertThat(counter).isNotNull();
		assertThat(counter.get()).isEqualTo(10L);
	}

	@Test
	public void should_not_get_typed_when_absent() throws Exception {
		TypedMap map = new TypedMap();

		Counter counter = map.<Counter> getTyped("key");

		assertThat(counter).isNull();
	}

	@Test
	public void should_return_null_when_null_value() throws Exception {
		TypedMap map = new TypedMap();
		map.put("key", null);
		Counter counter = map.<Counter> getTyped("key");

		assertThat(counter).isNull();
	}

	@Test
	public void should_get_typed_or_default() throws Exception {
		TypedMap map = new TypedMap();
		map.put("key", CounterBuilder.incr(11L));

		Counter counter = map.<Counter> getTypedOr("key", CounterBuilder.incr(10L));

		assertThat(counter).isNotNull();
		assertThat(counter.get()).isEqualTo(11L);
	}

	@Test
	public void should_return_default() throws Exception {
		TypedMap map = new TypedMap();

		Counter counter = map.<Counter> getTypedOr("key", CounterBuilder.incr(10L));

		assertThat(counter).isNotNull();
		assertThat(counter.get()).isEqualTo(10L);
	}

	@Test
	public void should_build_typed_map_from_source() throws Exception {
		Map<String, Object> source = new HashMap<>();
		source.put("string", "value");
		source.put("int", 10);

		TypedMap typedMap = TypedMap.fromMap(source);

		assertThat(typedMap.<String> getTyped("string")).isInstanceOf(String.class);
		assertThat(typedMap.<Integer> getTyped("int")).isInstanceOf(Integer.class);

	}
}
