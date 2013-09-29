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

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CQLMergerImplTest {
	@InjectMocks
	private CQLMergerImpl mergerImpl = new CQLMergerImpl();

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLEntityMerger entityMerger;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Captor
	private ArgumentCaptor<List<PropertyMeta>> pmCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private PropertyMeta idMeta;

	@Before
	public void setUp() throws Exception {
		when(context.getEntity()).thenReturn(entity);
		when(context.getEntityMeta()).thenReturn(entityMeta);

		idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors().build();
	}

	@Test
	public void should_merge() throws Exception {
		PropertyMeta ageMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("age").accessors()
				.build();
		Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();
		dirtyMap.put(idMeta.getGetter(), idMeta);
		dirtyMap.put(ageMeta.getGetter(), ageMeta);

		mergerImpl.merge(context, dirtyMap);

		assertThat(dirtyMap).isEmpty();

		verify(context).pushUpdateStatement(pmCaptor.capture());

		assertThat(pmCaptor.getValue()).containsExactly(ageMeta, idMeta);
	}

	@Test
	public void should_not_merge_when_empty_dirty_map() throws Exception {
		Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();
		mergerImpl.merge(context, dirtyMap);

		verifyZeroInteractions(context);
	}
}
