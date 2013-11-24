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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.LoaderImpl;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest {

	@InjectMocks
	private EntityLoader loader;

	@Mock
	private LoaderImpl loaderImpl;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private PersistenceContext context;

	private CompleteBean entity = new CompleteBean();

	private PropertyMeta idMeta;

	private Long primaryKey = RandomUtils.nextLong();

	@Before
	public void setUp() throws Exception {

		idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").accessors().invoker(invoker)
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setClusteredEntity(false);
		meta.setIdMeta(idMeta);
		meta.setEntityClass(CompleteBean.class);
		Whitebox.setInternalState(meta, ReflectionInvoker.class, invoker);

		when(context.getEntity()).thenReturn(entity);
		when(context.getEntityMeta()).thenReturn(meta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
	}

	@Test
	public void should_load_lazy_entity() throws Exception {

		when(context.isLoadEagerFields()).thenReturn(false);
		when(invoker.instantiate(CompleteBean.class)).thenReturn(entity);

		CompleteBean actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isSameAs(entity);

		verify(invoker).setValueToField(actual, idMeta.getSetter(), primaryKey);
	}

	@Test
	public void should_load_entity() throws Exception {
		when(context.isLoadEagerFields()).thenReturn(true);
		when(loaderImpl.eagerLoadEntity(context)).thenReturn(entity);

		CompleteBean actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isSameAs(entity);

		verify(invoker).setValueToField(actual, idMeta.getSetter(), primaryKey);
	}

	@Test
	public void should_load_property_into_object() throws Exception {
		when(proxifier.getRealObject(entity)).thenReturn(entity);

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Long.class).type(SIMPLE).build();

		loader.loadPropertyIntoObject(context, entity, pm);

		verify(loaderImpl).loadPropertyIntoEntity(context, pm, entity);
	}

	@Test
	public void should_not_load_property_into_object_for_proxy_type() throws Exception {
		when(proxifier.getRealObject(entity)).thenReturn(entity);

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Counter.class).type(COUNTER).build();

		loader.loadPropertyIntoObject(context, entity, pm);

		verifyZeroInteractions(loaderImpl);
	}
}
