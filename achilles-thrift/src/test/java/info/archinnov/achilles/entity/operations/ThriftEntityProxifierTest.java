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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityProxifierTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();

	@Mock
	private ThriftPersistenceContext context;

	@Test
	public void should_build_interceptor() throws Exception {
		ThriftGenericEntityDao entityDao = mock(ThriftGenericEntityDao.class);
		CompleteBean entity = new CompleteBean();
		Long primaryKey = 11L;
		entity.setId(primaryKey);

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors()
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setGetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setSetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		when(context.getEntityMeta()).thenReturn(meta);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityDao()).thenReturn(entityDao);
		when(context.getPrimaryKey()).thenReturn(primaryKey);

		ThriftEntityInterceptor<CompleteBean> interceptor = proxifier.buildInterceptor(context, entity,
				new HashSet<Method>());

		assertThat(interceptor).isInstanceOf(ThriftEntityInterceptor.class);
		assertThat(interceptor.getTarget()).isSameAs(entity);
		assertThat(interceptor.getContext()).isSameAs(context);

		assertThat(interceptor.getDirtyMap()).isNotNull();
		assertThat(interceptor.getDirtyMap()).isEmpty();

		assertThat(interceptor.getPrimaryKey()).isSameAs(primaryKey);
		assertThat(interceptor.getAlreadyLoaded()).isNotNull();
		assertThat(interceptor.getAlreadyLoaded()).isEmpty();
	}

	@Test
	public void should_exception_when_no_wide_row_dao_found() throws Exception {
		CompleteBean entity = new CompleteBean();

		EntityMeta meta = new EntityMeta();
		meta.setGetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setSetterMetas(new HashMap<Method, PropertyMeta>());
		meta.setClassName("CompleteBean");
		meta.setClusteredEntity(true);

		when(context.getEntityMeta()).thenReturn(meta);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);

		exception.expect(AchillesException.class);

		proxifier.buildInterceptor(context, entity, new HashSet<Method>());
	}
}
