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

import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityRefresherTest {

	@InjectMocks
	private EntityRefresher<PersistenceContext> entityRefresher;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityProxifier<PersistenceContext> proxifier;

	@Mock
	private EntityLoader<PersistenceContext> loader;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private EntityInterceptor<PersistenceContext, CompleteBean> jpaEntityInterceptor;

	@Mock
	private Map<Method, PropertyMeta> dirtyMap;

	@Mock
	private Set<Method> alreadyLoaded;

	@Mock
	private PersistenceContext context;

	@Test
	public void should_refresh() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		List<Method> eagerGetters = new ArrayList<Method>();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(
				CompleteBean.class);
		when(context.getPrimaryKey()).thenReturn(bean.getId());
		when(context.getEntity()).thenReturn(bean);

		when(proxifier.getInterceptor(bean)).thenReturn(jpaEntityInterceptor);

		when(jpaEntityInterceptor.getTarget()).thenReturn(bean);
		when(jpaEntityInterceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(jpaEntityInterceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(entityMeta.getEagerGetters()).thenReturn(eagerGetters);
		when(loader.load(context, CompleteBean.class)).thenReturn(bean);

		entityRefresher.refresh(context);

		verify(dirtyMap).clear();
		verify(alreadyLoaded).clear();
		verify(alreadyLoaded).addAll(eagerGetters);
		verify(jpaEntityInterceptor).setTarget(bean);
	}

	@Test(expected = AchillesStaleObjectStateException.class)
	public void should_throw_exception_when_object_staled() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		List<Method> eagerGetters = new ArrayList<Method>();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(
				CompleteBean.class);
		when(context.getPrimaryKey()).thenReturn(bean.getId());
		when(context.getEntity()).thenReturn(bean);

		when(proxifier.getInterceptor(bean)).thenReturn(jpaEntityInterceptor);

		when(jpaEntityInterceptor.getTarget()).thenReturn(bean);
		when(jpaEntityInterceptor.getDirtyMap()).thenReturn(dirtyMap);
		when(jpaEntityInterceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(entityMeta.getEagerGetters()).thenReturn(eagerGetters);
		when(loader.load(context, CompleteBean.class)).thenReturn(null);

		entityRefresher.refresh(context);
	}
}
