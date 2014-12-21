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

import org.junit.Test;

public class PropertyTypeTest {


	@Test
	public void should_test_is_counter() throws Exception {
		assertThat(PropertyType.PARTITION_KEY.isCounter()).isFalse();
		assertThat(PropertyType.SIMPLE.isCounter()).isFalse();
		assertThat(PropertyType.LIST.isCounter()).isFalse();
		assertThat(PropertyType.MAP.isCounter()).isFalse();
		assertThat(PropertyType.COUNTER.isCounter()).isTrue();
		assertThat(PropertyType.COMPOUND_PRIMARY_KEY.isCounter()).isFalse();
	}

	@Test
	public void should_test_is_id() throws Exception {
		assertThat(PropertyType.COUNTER.isPrimaryKey()).isFalse();

		assertThat(PropertyType.PARTITION_KEY.isPrimaryKey()).isTrue();
		assertThat(PropertyType.SIMPLE.isPrimaryKey()).isFalse();
		assertThat(PropertyType.LIST.isPrimaryKey()).isFalse();
		assertThat(PropertyType.MAP.isPrimaryKey()).isFalse();
		assertThat(PropertyType.COMPOUND_PRIMARY_KEY.isPrimaryKey()).isTrue();
	}

	@Test
	public void should_test_is_compound_pk() throws Exception {
		assertThat(PropertyType.COUNTER.isCompoundPK()).isFalse();

		assertThat(PropertyType.PARTITION_KEY.isCompoundPK()).isFalse();
		assertThat(PropertyType.SIMPLE.isCompoundPK()).isFalse();
		assertThat(PropertyType.LIST.isCompoundPK()).isFalse();
		assertThat(PropertyType.MAP.isCompoundPK()).isFalse();
		assertThat(PropertyType.COMPOUND_PRIMARY_KEY.isCompoundPK()).isTrue();
	}

	@Test
	public void should_test_is_valid_clustered_value() throws Exception {
		assertThat(PropertyType.COUNTER.isValidClusteredValueType()).isTrue();

		assertThat(PropertyType.PARTITION_KEY.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.SIMPLE.isValidClusteredValueType()).isTrue();
		assertThat(PropertyType.LIST.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.MAP.isValidClusteredValueType()).isFalse();
		assertThat(PropertyType.COMPOUND_PRIMARY_KEY.isValidClusteredValueType()).isFalse();
	}

    @Test
    public void should_test_is_collection_and_map() throws Exception {
        assertThat(PropertyType.PARTITION_KEY.isCollectionAndMap()).isFalse();
        assertThat(PropertyType.SIMPLE.isCollectionAndMap()).isFalse();
        assertThat(PropertyType.LIST.isCollectionAndMap()).isTrue();
        assertThat(PropertyType.SET.isCollectionAndMap()).isTrue();
        assertThat(PropertyType.MAP.isCollectionAndMap()).isTrue();
        assertThat(PropertyType.COUNTER.isCollectionAndMap()).isFalse();
        assertThat(PropertyType.COMPOUND_PRIMARY_KEY.isCollectionAndMap()).isFalse();
    }
}
