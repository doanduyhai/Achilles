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
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftMergerImplTest {

	@InjectMocks
	private ThriftMergerImpl mergerImpl;

	@Mock
	private ThriftEntityPersister persister;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private ThriftEntityMerger entityMerger;

	@Mock
	private ThriftPersistenceContext context;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private EntityMeta meta = new EntityMeta();

	private Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();

	@Before
	public void setUp() {
		when(context.getEntity()).thenReturn(entity);
		when(context.getEntityMeta()).thenReturn(meta);

		meta.setClusteredEntity(false);
		dirtyMap.clear();
	}

	@Test
	public void should_merge_simple_property() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		dirtyMap.put(pm.getSetter(), pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn("name");

		mergerImpl.merge(context, dirtyMap);

		verify(persister).persistPropertyBatch(context, pm);
	}

	@Test
	public void should_merge_multi_values_property() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends").accessors()
				.type(LIST).invoker(invoker).build();

		dirtyMap.put(pm.getSetter(), pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(Arrays.asList("friends"));

		mergerImpl.merge(context, dirtyMap);

		verify(persister).removePropertyBatch(context, pm);
		verify(persister).persistPropertyBatch(context, pm);
	}

	@Test
	public void should_remove_property_when_null() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		dirtyMap.put(pm.getSetter(), pm);

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(null);

		mergerImpl.merge(context, dirtyMap);

		verify(persister).removePropertyBatch(context, pm);
		verify(persister, never()).persistPropertyBatch(context, pm);
	}

	@Test
	public void should_remove_clustered_entity_when_value_dirty() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		dirtyMap.put(pm.getSetter(), pm);
		meta.setClusteredEntity(true);
		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(null);

		mergerImpl.merge(context, dirtyMap);

		verify(persister).remove(context);
	}

	@Test
	public void should_merge_value_for_clustered_entity() throws Exception {
		Object clusteredValue = "clusteredValue";
		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		dirtyMap.put(pm.getSetter(), pm);
		meta.setClusteredEntity(true);
		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(clusteredValue);

		mergerImpl.merge(context, dirtyMap);

		verify(persister).persistClusteredValue(context, clusteredValue);

	}

	@Test
	public void should_do_nothing_when_not_dirty() throws Exception {
		mergerImpl.merge(context, dirtyMap);

		verifyZeroInteractions(context, invoker, persister);
	}
}
