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
package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.Merger;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityMergerTest {

	@InjectMocks
	private EntityMerger<PersistenceContext> entityMerger = new EntityMerger<PersistenceContext>() {
	};

	@Mock
	private Merger<PersistenceContext> merger;

	@Mock
	private EntityPersister<PersistenceContext> persister;

	@Mock
	private EntityProxifier<PersistenceContext> proxifier;

	@Mock
	private PersistenceContext context;

	@Mock
	private EntityInterceptor<PersistenceContext, CompleteBean> interceptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private EntityMeta meta = new EntityMeta();

	private List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();

	private Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		Whitebox.setInternalState(entityMerger, Merger.class, merger);
		Whitebox.setInternalState(entityMerger, EntityPersister.class, persister);
		Whitebox.setInternalState(entityMerger, EntityProxifier.class, proxifier);

		when(context.getEntity()).thenReturn(entity);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);

		allMetas.clear();
		dirtyMap.clear();
	}

	@Test
	public void should_merge_proxified_entity() throws Exception {
		when(proxifier.isProxy(entity)).thenReturn(true);
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).field("user").type(SIMPLE)
				.accessors().build();

		meta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta> asList(pm));

		dirtyMap.put(pm.getSetter(), pm);

		CompleteBean actual = entityMerger.merge(context, entity);

		assertThat(actual).isSameAs(entity);
		verify(context).setEntity(entity);
		verify(merger).merge(context, dirtyMap);

		verify(interceptor).setContext(context);
		verify(interceptor).setTarget(entity);

	}

	@Test
	public void should_persist_transient_entity() throws Exception {
		when(proxifier.isProxy(entity)).thenReturn(false);
		when(context.isClusteredEntity()).thenReturn(false);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		CompleteBean actual = entityMerger.merge(context, entity);

		assertThat(actual).isSameAs(entity);
		verify(persister).persist(context);
	}

}
