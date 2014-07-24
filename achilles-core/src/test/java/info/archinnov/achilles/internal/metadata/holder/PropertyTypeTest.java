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
package info.archinnov.achilles.internal.metadata.holder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;

import org.junit.Test;

public class PropertyTypeTest {


	@Test
	public void should_test_is_counter() throws Exception {
		assertThat(PropertyType.ID.isCounter()).isFalse();
		assertThat(PropertyType.SIMPLE.isCounter()).isFalse();
		assertThat(PropertyType.LIST.isCounter()).isFalse();
		assertThat(PropertyType.MAP.isCounter()).isFalse();
		assertThat(PropertyType.COUNTER.isCounter()).isTrue();
		assertThat(PropertyType.EMBEDDED_ID.isCounter()).isFalse();
	}

	@Test
	public void should_test_is_id() throws Exception {
		assertThat(PropertyType.COUNTER.isId()).isFalse();

		assertThat(PropertyType.ID.isId()).isTrue();
		assertThat(PropertyType.SIMPLE.isId()).isFalse();
		assertThat(PropertyType.LIST.isId()).isFalse();
		assertThat(PropertyType.MAP.isId()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isId()).isTrue();
	}

	@Test
	public void should_test_is_embedded_id() throws Exception {
		assertThat(PropertyType.COUNTER.isEmbeddedId()).isFalse();

		assertThat(PropertyType.ID.isEmbeddedId()).isFalse();
		assertThat(PropertyType.SIMPLE.isEmbeddedId()).isFalse();
		assertThat(PropertyType.LIST.isEmbeddedId()).isFalse();
		assertThat(PropertyType.MAP.isEmbeddedId()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isEmbeddedId()).isTrue();
	}

	@Test
	public void should_test_is_valid_clustered_value() throws Exception {
		assertThat(PropertyType.COUNTER.isValidClusteredValueType()).isTrue();

		assertThat(PropertyType.ID.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.SIMPLE.isValidClusteredValueType()).isTrue();
		assertThat(PropertyType.LIST.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.MAP.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.EMBEDDED_ID.isValidClusteredValueType()).isFalse();
	}

    @Test
    public void should_test_is_collection_and_map() throws Exception {
        assertThat(PropertyType.ID.isCollectionAndMap()).isFalse();
        assertThat(PropertyType.SIMPLE.isCollectionAndMap()).isFalse();
        assertThat(PropertyType.LIST.isCollectionAndMap()).isTrue();
        assertThat(PropertyType.SET.isCollectionAndMap()).isTrue();
        assertThat(PropertyType.MAP.isCollectionAndMap()).isTrue();
        assertThat(PropertyType.COUNTER.isCollectionAndMap()).isFalse();
        assertThat(PropertyType.EMBEDDED_ID.isCollectionAndMap()).isFalse();
    }
}
