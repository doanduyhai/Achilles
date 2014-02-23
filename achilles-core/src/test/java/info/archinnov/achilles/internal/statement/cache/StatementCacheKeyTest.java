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
package info.archinnov.achilles.internal.statement.cache;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import org.junit.Test;

import com.google.common.collect.Sets;

public class StatementCacheKeyTest {

	@Test
	public void should_be_equals() throws Exception {
		StatementCacheKey key1 = new StatementCacheKey(CacheType.SELECT_FIELD, Sets.newHashSet("field1",
				"field2"), CompleteBean.class);
		StatementCacheKey key2 = new StatementCacheKey(CacheType.SELECT_FIELD, Sets.newHashSet("field2",
				"field1"), CompleteBean.class);

		assertThat(key1).isEqualTo(key2);
	}
}
