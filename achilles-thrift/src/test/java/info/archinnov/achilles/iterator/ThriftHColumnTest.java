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
		ThriftHColumn<Integer, String> hCol = new ThriftHColumn<Integer, String>();

		hCol.setName(11);
		hCol.setValue("val");
		hCol.setClock(1000);
		hCol.setTtl(10);

		assertThat(hCol.getName()).isEqualTo(11);
		assertThat(hCol.getValue()).isEqualTo("val");
		assertThat(hCol.getTtl()).isEqualTo(10);
		assertThat(hCol.getClock()).isEqualTo(0);

		assertThat(hCol.getNameBytes()).isNull();
		assertThat(hCol.getValueBytes()).isNull();
		assertThat(hCol.getNameSerializer()).isNull();
		assertThat(hCol.getValueSerializer()).isNull();

		hCol.apply("val2", 1, 20);

		assertThat(hCol.getName()).isEqualTo(11);
		assertThat(hCol.getValue()).isEqualTo("val2");
		assertThat(hCol.getTtl()).isEqualTo(20);
		assertThat(hCol.getClock()).isEqualTo(0);

		hCol.clear();

		assertThat(hCol.getName()).isNull();
		assertThat(hCol.getValue()).isNull();
		assertThat(hCol.getTtl()).isEqualTo(0);
		assertThat(hCol.getClock()).isEqualTo(0);

	}
}
