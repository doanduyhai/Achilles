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
package info.archinnov.achilles.internal.metadata.parsing;

import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Field;

import info.archinnov.achilles.annotations.PartitionKey;
import org.junit.Test;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.ParentBean;

public class PropertyFilterTest {
	private PropertyFilter filter = new PropertyFilter();

	@Test
	public void should_match() throws Exception {
		Field name = CompleteBean.class.getDeclaredField("name");

		assertThat(filter.matches(name)).isTrue();
	}

    @Test
    public void should_match_partition_key() throws Exception {
        Field name = CompleteBean.class.getDeclaredField("id");

        assertThat(filter.matches(name)).isTrue();
    }

	@Test
	public void should_match_compound_pk() throws Exception {
		Field clusteredId = BeanWithClusteredId.class.getDeclaredField("id");

		assertThat(filter.matches(clusteredId)).isTrue();
	}

	@Test
	public void should_match_annotation() throws Exception {
		Field friends = CompleteBean.class.getDeclaredField("friends");

		assertThat(filter.matches(friends, Column.class)).isTrue();
	}

	@Test
	public void should_match_annotation_and_name() throws Exception {
		Field friends = CompleteBean.class.getDeclaredField("friends");

		assertThat(filter.matches(friends, Column.class, "friends")).isTrue();
	}

	@Test
	public void should_not_match() throws Exception {
		Field name = ParentBean.class.getDeclaredField("unmapped");

		assertThat(filter.matches(name)).isFalse();
	}

	@Test
	public void should_have_annotation() throws Exception {
		Field name = CompleteBean.class.getDeclaredField("name");

		assertThat(filter.hasAnnotation(name, Column.class)).isTrue();
	}

	@Test
	public void should_not_have_annotation() throws Exception {
		Field name = CompleteBean.class.getDeclaredField("name");

		assertThat(filter.hasAnnotation(name, PartitionKey.class)).isFalse();
	}
}
