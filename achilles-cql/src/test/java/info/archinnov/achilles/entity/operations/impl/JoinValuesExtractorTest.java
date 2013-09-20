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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class JoinValuesExtractorTest {
	private JoinValuesExtractor transformer;

	@Mock
	private ReflectionInvoker invoker;

	private CompleteBean entity = new CompleteBean();
	private UserBean user1 = new UserBean();
	private UserBean user2 = new UserBean();

	@Before
	public void setUp() {
		transformer = new JoinValuesExtractor(entity);
	}

	@Test
	public void should_transform_join_simple_meta() throws Exception {
		PropertyMeta joinSimpleMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(JOIN_SIMPLE).accessors().invoker(invoker).build();

		when(invoker.getValueFromField(entity, joinSimpleMeta.getGetter()))
				.thenReturn(user1);

		Collection<Pair<List<?>, PropertyMeta>> pairsList = Collections2
				.transform(Arrays.asList(joinSimpleMeta), transformer);

		assertThat(pairsList).hasSize(1);
		Pair<List<?>, PropertyMeta> pair = pairsList.iterator().next();
		assertThat((List<Object>) pair.left).containsOnly(user1);
		assertThat((PropertyMeta) pair.right).isSameAs(joinSimpleMeta);
	}

	@Test
	public void should_transform_join_collection_meta() throws Exception {
		PropertyMeta joinListMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(JOIN_LIST).accessors().invoker(invoker).build();

		List<UserBean> joinUsers = Arrays.asList(user1, user2);

		when(invoker.getValueFromField(entity, joinListMeta.getGetter()))
				.thenReturn(joinUsers);

		Collection<Pair<List<?>, PropertyMeta>> pairsList = Collections2
				.transform(Arrays.asList(joinListMeta), transformer);

		assertThat(pairsList).hasSize(1);
		Pair<List<?>, PropertyMeta> pair = pairsList.iterator().next();
		assertThat((List<Object>) pair.left).containsOnly(user1, user2);
		assertThat((PropertyMeta) pair.right).isSameAs(joinListMeta);
	}

	@Test
	public void should_transform_join_map_meta() throws Exception {
		PropertyMeta joinMapMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(JOIN_MAP).accessors().invoker(invoker).build();

		Map<Integer, UserBean> joinUsers = ImmutableMap.of(1, user1, 2, user2);

		when(invoker.getValueFromField(entity, joinMapMeta.getGetter()))
				.thenReturn(joinUsers);

		Collection<Pair<List<?>, PropertyMeta>> pairsList = Collections2
				.transform(Arrays.asList(joinMapMeta), transformer);

		assertThat(pairsList).hasSize(1);
		Pair<List<?>, PropertyMeta> pair = pairsList.iterator().next();
		assertThat((List<Object>) pair.left).containsOnly(user1, user2);
		assertThat((PropertyMeta) pair.right).isSameAs(joinMapMeta);
	}
}
