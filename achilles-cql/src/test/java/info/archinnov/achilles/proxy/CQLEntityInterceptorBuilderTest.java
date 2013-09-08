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
package info.archinnov.achilles.proxy;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.HashMap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityInterceptorBuilderTest {

	@Mock
	CQLPersistenceContext context;

	private CompleteBean entity = new CompleteBean();

	@Test
	public void should_build_interceptor_with_eager_fields_already_loaded()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("id").build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setClassName("classname");
		meta.setGetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setSetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setEagerGetters(Lists.newArrayList(idMeta.getGetter()));

		when((Class) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(context.getPrimaryKey()).thenReturn(entity.getId());
		when(context.isLoadEagerFields()).thenReturn(true);

		CQLEntityInterceptor<CompleteBean> interceptor = CQLEntityInterceptorBuilder
				.<CompleteBean> builder(context, entity).build();

		assertThat(interceptor.getContext()).isSameAs(context);
		assertThat(interceptor.getTarget()).isSameAs(entity);
		assertThat(interceptor.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getAlreadyLoaded()).containsOnly(
				idMeta.getGetter());
	}

	@Test
	public void should_build_interceptor_with_no_eager_fields()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("id").build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setClassName("classname");
		meta.setGetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setSetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setEagerGetters(Lists.newArrayList(idMeta.getGetter()));

		when((Class) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(context.getPrimaryKey()).thenReturn(entity.getId());
		when(context.isLoadEagerFields()).thenReturn(false);

		CQLEntityInterceptor<CompleteBean> interceptor = CQLEntityInterceptorBuilder
				.<CompleteBean> builder(context, entity).build();

		assertThat(interceptor.getContext()).isSameAs(context);
		assertThat(interceptor.getTarget()).isSameAs(entity);
		assertThat(interceptor.getPrimaryKey()).isEqualTo(entity.getId());
		assertThat(interceptor.getAlreadyLoaded()).isEmpty();
	}
}
