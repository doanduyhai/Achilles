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
package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class ThriftHColumnTest {

	@Test
	public void should_test_all_methods() throws Exception {
		ThriftHColumn<Integer, String> joinHCol = new ThriftHColumn<Integer, String>();

		joinHCol.setName(11);
		joinHCol.setValue("val");
		joinHCol.setClock(1000);
		joinHCol.setTtl(10);

		assertThat(joinHCol.getName()).isEqualTo(11);
		assertThat(joinHCol.getValue()).isEqualTo("val");
		assertThat(joinHCol.getTtl()).isEqualTo(10);
		assertThat(joinHCol.getClock()).isEqualTo(0);

		assertThat(joinHCol.getNameBytes()).isNull();
		assertThat(joinHCol.getValueBytes()).isNull();
		assertThat(joinHCol.getNameSerializer()).isNull();
		assertThat(joinHCol.getValueSerializer()).isNull();

		joinHCol.apply("val2", 1, 20);

		assertThat(joinHCol.getName()).isEqualTo(11);
		assertThat(joinHCol.getValue()).isEqualTo("val2");
		assertThat(joinHCol.getTtl()).isEqualTo(20);
		assertThat(joinHCol.getClock()).isEqualTo(0);

		joinHCol.clear();

		assertThat(joinHCol.getName()).isNull();
		assertThat(joinHCol.getValue()).isNull();
		assertThat(joinHCol.getTtl()).isEqualTo(0);
		assertThat(joinHCol.getClock()).isEqualTo(0);

	}
}
