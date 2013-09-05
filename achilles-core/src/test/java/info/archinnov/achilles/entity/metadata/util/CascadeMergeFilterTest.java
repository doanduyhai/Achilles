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
package info.archinnov.achilles.entity.metadata.util;

import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.Collections2;

public class CascadeMergeFilterTest {
	@Test
	public void should_filter_cascade_merge_or_all() throws Exception {
		CascadeMergeFilter filter = new CascadeMergeFilter();
		PropertyMeta pm1 = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").cascadeTypes(MERGE, PERSIST).build();
		PropertyMeta pm2 = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").cascadeTypes(REMOVE, REFRESH).build();

		assertThat(Collections2.filter(Arrays.asList(pm1), filter))
				.containsExactly(pm1);
		assertThat(Collections2.filter(Arrays.asList(pm2), filter)).isEmpty();
	}
}
