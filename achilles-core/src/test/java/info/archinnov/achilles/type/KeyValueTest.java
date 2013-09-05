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

public class KeyValueTest {
	@Test
	public void should_to_string() throws Exception {
		KeyValue<Integer, String> kv = new KeyValue<Integer, String>(11,
				"value", 1, 10L);

		assertThat(kv.toString()).isEqualTo(
				"KeyValue [key=11, value=value, ttl=1, timestamp=10]");
	}
}
