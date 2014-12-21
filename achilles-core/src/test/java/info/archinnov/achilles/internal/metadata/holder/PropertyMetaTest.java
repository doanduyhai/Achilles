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

import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTest {

	@Test
	public void should_use_equals_and_hashcode() throws Exception {
		PropertyMeta meta1 = new PropertyMeta();
		meta1.setEntityClassName("entity");
		meta1.setPropertyName("field1");
		meta1.setType(PropertyType.SIMPLE);

		PropertyMeta meta2 = new PropertyMeta();
		meta2.setEntityClassName("entity");
		meta2.setPropertyName("field2");
		meta2.setType(PropertyType.SIMPLE);

		PropertyMeta meta3 = new PropertyMeta();
		meta3.setEntityClassName("entity");
		meta3.setPropertyName("field1");
		meta3.setType(PropertyType.LIST);

		PropertyMeta meta4 = new PropertyMeta();
		meta4.setEntityClassName("entity1");
		meta4.setPropertyName("field1");
		meta4.setType(PropertyType.SIMPLE);

		PropertyMeta meta5 = new PropertyMeta();
		meta5.setEntityClassName("entity");
		meta5.setPropertyName("field1");
		meta5.setType(PropertyType.SIMPLE);

		assertThat(meta1).isNotEqualTo(meta2);
		assertThat(meta1).isNotEqualTo(meta3);
		assertThat(meta1).isNotEqualTo(meta4);
		assertThat(meta1).isEqualTo(meta5);

		assertThat(meta1.hashCode()).isNotEqualTo(meta2.hashCode());
		assertThat(meta1.hashCode()).isNotEqualTo(meta3.hashCode());
		assertThat(meta1.hashCode()).isNotEqualTo(meta4.hashCode());
		assertThat(meta1.hashCode()).isEqualTo(meta5.hashCode());

		Set<PropertyMeta> pms = Sets.newHashSet(meta1, meta2, meta3, meta4, meta5);

		assertThat(pms).containsOnly(meta1, meta2, meta3, meta4);
	}
}
