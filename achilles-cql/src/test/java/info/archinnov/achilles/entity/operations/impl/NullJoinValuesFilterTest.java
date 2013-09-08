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
package info.archinnov.achilles.entity.operations.impl;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.utils.Pair;
import org.junit.Test;
import org.mockito.Mock;

import com.google.common.collect.Collections2;

public class NullJoinValuesFilterTest {

	private NullJoinValuesFilter filter = new NullJoinValuesFilter();

	@Mock
	private ReflectionInvoker invoker;

	@Test
	public void should_return_list_when_join_value_exist() throws Exception {
		PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(PropertyType.JOIN_SIMPLE).accessors().build();
		UserBean user = new UserBean();

		Pair<List<?>, PropertyMeta> pair = Pair.<List<?>, PropertyMeta> create(
				Arrays.asList(user), joinSimpleMeta);
		Collection<Pair<List<?>, PropertyMeta>> filtered = Collections2.filter(
				Arrays.asList(pair), filter);

		assertThat(filtered).hasSize(1);

	}

	@Test
	public void should_filter_out_empty_join_values_list() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		UserBean user = new UserBean();

		Pair<List<?>, PropertyMeta> pair1 = Pair
				.<List<?>, PropertyMeta> create(Arrays.asList(), pm);
		Pair<List<?>, PropertyMeta> pair2 = Pair
				.<List<?>, PropertyMeta> create(Arrays.asList(user), pm);

		List<Pair<List<?>, PropertyMeta>> list = Arrays.asList(pair1, pair2);

		Collection<Pair<List<?>, PropertyMeta>> filtered = Collections2.filter(
				list, filter);

		assertThat(filtered).containsOnly(pair2);

	}
}
