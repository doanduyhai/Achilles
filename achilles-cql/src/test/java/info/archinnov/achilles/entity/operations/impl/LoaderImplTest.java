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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.proxy.RowMethodInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Row;
import com.google.common.base.Optional;

@RunWith(MockitoJUnitRunner.class)
public class LoaderImplTest {
	@InjectMocks
	private LoaderImpl loaderImpl;

	@Mock
	private EntityMapper mapper;

	@Mock
	private EntityLoader entityLoader;

	@Mock
	private RowMethodInvoker cqlRowInvoker;

	@Mock
	private Row row;

	@Mock
	private PersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private ReflectionInvoker invoker;

	@Captor
	private ArgumentCaptor<List<UserBean>> listCaptor;

	@Captor
	private ArgumentCaptor<Set<UserBean>> setCaptor;

	@Captor
	private ArgumentCaptor<Map<Integer, UserBean>> mapCaptor;

	private PropertyMeta idMeta;

	@Before
	public void setUp() throws Exception {
		when(context.getEntityMeta()).thenReturn(entityMeta);
		idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(PropertyType.ID)
				.accessors().build();

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.isClusteredCounter()).thenReturn(false);
	}

	@Test
	public void should_eager_load_entity() throws Exception {
		when(context.eagerLoadEntity()).thenReturn(row);
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(entityMeta.instanciate()).thenReturn(new CompleteBean());

		CompleteBean actual = loaderImpl.eagerLoadEntity(context);

		assertThat(actual).isInstanceOf(CompleteBean.class);

		verify(mapper).setEagerPropertiesToEntity(row, entityMeta, actual);
	}

	@Test
	public void should_return_null_for_eager_load_when_not_found() throws Exception {
		when(context.eagerLoadEntity()).thenReturn(null);

		CompleteBean actual = loaderImpl.eagerLoadEntity(context);

		assertThat(actual).isNull();
		verifyZeroInteractions(mapper);
	}

	@Test
	public void should_eager_load_clustered_counter_entity_with_runtime_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();

		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.type(PropertyType.COUNTER).build();
		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(entityMeta.getFirstMeta()).thenReturn(counterMeta);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(context.getClusteredCounter(counterMeta, EACH_QUORUM)).thenReturn(counterValue);
		when(entityMeta.instanciate()).thenReturn(new CompleteBean());

		CompleteBean actual = loaderImpl.eagerLoadEntity(context);

		assertThat(actual).isInstanceOf(CompleteBean.class);

		verifyZeroInteractions(mapper);
	}

	@Test
	public void should_eager_load_clustered_counter_entity_with_default_consistency() throws Exception {
		Long counterValue = RandomUtils.nextLong();

		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.type(PropertyType.COUNTER).consistencyLevels(Pair.create(EACH_QUORUM, EACH_QUORUM)).build();
		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(entityMeta.getFirstMeta()).thenReturn(counterMeta);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(null));
		when(context.getClusteredCounter(counterMeta, EACH_QUORUM)).thenReturn(counterValue);
		when(entityMeta.instanciate()).thenReturn(new CompleteBean());

		CompleteBean actual = loaderImpl.eagerLoadEntity(context);

		assertThat(actual).isInstanceOf(CompleteBean.class);

		verifyZeroInteractions(mapper);
	}

	@Test
	public void should_return_null_for_eager_load_clusterd_counter_when_not_found() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.type(PropertyType.COUNTER).build();

		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(entityMeta.getFirstMeta()).thenReturn(counterMeta);
		when(context.getConsistencyLevel()).thenReturn(Optional.<ConsistencyLevel> fromNullable(EACH_QUORUM));
		when(context.getClusteredCounter(counterMeta, EACH_QUORUM)).thenReturn(null);

		CompleteBean actual = loaderImpl.eagerLoadEntity(context);

		assertThat(actual).isNull();
	}

	@Test
	public void should_load_property_into_entity() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).field("name").type(PropertyType.SIMPLE)
				.build();

		CompleteBean entity = new CompleteBean();
		when(context.loadProperty(pm)).thenReturn(row);

		loaderImpl.loadPropertyIntoEntity(context, pm, entity);

		verify(mapper).setPropertyToEntity(row, pm, entity);
	}
}
