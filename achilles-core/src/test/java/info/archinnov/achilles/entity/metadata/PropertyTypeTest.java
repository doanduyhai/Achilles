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
package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class PropertyTypeTest {

	@Test
	public void should_test_is_lazy() throws Exception {
		assertThat(PropertyType.COUNTER.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_SIMPLE.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_LIST.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_SET.isLazy()).isTrue();
		assertThat(PropertyType.LAZY_MAP.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_SIMPLE.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_LIST.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_SET.isLazy()).isTrue();
		assertThat(PropertyType.JOIN_MAP.isLazy()).isTrue();

		assertThat(PropertyType.ID.isLazy()).isFalse();
		assertThat(PropertyType.SIMPLE.isLazy()).isFalse();
		assertThat(PropertyType.LIST.isLazy()).isFalse();
		assertThat(PropertyType.MAP.isLazy()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isLazy()).isFalse();
	}

	@Test
	public void should_test_is_join_column() throws Exception {
		assertThat(PropertyType.ID.isJoin()).isFalse();
		assertThat(PropertyType.SIMPLE.isJoin()).isFalse();
		assertThat(PropertyType.LIST.isJoin()).isFalse();
		assertThat(PropertyType.MAP.isJoin()).isFalse();
		assertThat(PropertyType.COUNTER.isJoin()).isFalse();
		assertThat(PropertyType.LAZY_SIMPLE.isJoin()).isFalse();
		assertThat(PropertyType.LAZY_LIST.isJoin()).isFalse();
		assertThat(PropertyType.LAZY_SET.isJoin()).isFalse();
		assertThat(PropertyType.LAZY_MAP.isJoin()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isJoin()).isFalse();

		assertThat(PropertyType.JOIN_SIMPLE.isJoin()).isTrue();
		assertThat(PropertyType.JOIN_LIST.isJoin()).isTrue();
		assertThat(PropertyType.JOIN_SET.isJoin()).isTrue();
		assertThat(PropertyType.JOIN_MAP.isJoin()).isTrue();
	}

	@Test
	public void should_test_is_counter() throws Exception {
		assertThat(PropertyType.ID.isCounter()).isFalse();
		assertThat(PropertyType.SIMPLE.isCounter()).isFalse();
		assertThat(PropertyType.LIST.isCounter()).isFalse();
		assertThat(PropertyType.MAP.isCounter()).isFalse();
		assertThat(PropertyType.COUNTER.isCounter()).isTrue();
		assertThat(PropertyType.LAZY_SIMPLE.isCounter()).isFalse();
		assertThat(PropertyType.LAZY_LIST.isCounter()).isFalse();
		assertThat(PropertyType.LAZY_SET.isCounter()).isFalse();
		assertThat(PropertyType.LAZY_MAP.isCounter()).isFalse();
		assertThat(PropertyType.JOIN_SIMPLE.isCounter()).isFalse();
		assertThat(PropertyType.JOIN_LIST.isCounter()).isFalse();
		assertThat(PropertyType.JOIN_SET.isCounter()).isFalse();
		assertThat(PropertyType.JOIN_MAP.isCounter()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isCounter()).isFalse();
	}

	@Test
	public void should_test_is_multikey() throws Exception {
		assertThat(PropertyType.COUNTER.isEmbeddedId()).isFalse();
		assertThat(PropertyType.LAZY_SIMPLE.isEmbeddedId()).isFalse();
		assertThat(PropertyType.LAZY_LIST.isEmbeddedId()).isFalse();
		assertThat(PropertyType.LAZY_SET.isEmbeddedId()).isFalse();
		assertThat(PropertyType.LAZY_MAP.isEmbeddedId()).isFalse();
		assertThat(PropertyType.JOIN_SIMPLE.isEmbeddedId()).isFalse();
		assertThat(PropertyType.JOIN_LIST.isEmbeddedId()).isFalse();
		assertThat(PropertyType.JOIN_SET.isEmbeddedId()).isFalse();
		assertThat(PropertyType.JOIN_MAP.isEmbeddedId()).isFalse();

		assertThat(PropertyType.ID.isEmbeddedId()).isFalse();
		assertThat(PropertyType.SIMPLE.isEmbeddedId()).isFalse();
		assertThat(PropertyType.LIST.isEmbeddedId()).isFalse();
		assertThat(PropertyType.MAP.isEmbeddedId()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isEmbeddedId()).isTrue();
	}

	@Test
	public void should_test_is_valid_clustered_value() throws Exception {
		assertThat(PropertyType.COUNTER.isValidClusteredValueType()).isTrue();
		assertThat(PropertyType.LAZY_SIMPLE.isValidClusteredValueType())
				.isFalse();
		assertThat(PropertyType.LAZY_LIST.isValidClusteredValueType())
				.isFalse();
		assertThat(PropertyType.LAZY_SET.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.LAZY_MAP.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.JOIN_SIMPLE.isValidClusteredValueType())
				.isTrue();
		assertThat(PropertyType.JOIN_LIST.isValidClusteredValueType())
				.isFalse();
		assertThat(PropertyType.JOIN_SET.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.JOIN_MAP.isValidClusteredValueType()).isFalse();

		assertThat(PropertyType.ID.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.SIMPLE.isValidClusteredValueType()).isTrue();
		assertThat(PropertyType.LIST.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.MAP.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isValidClusteredValueType())
				.isFalse();
	}
}
