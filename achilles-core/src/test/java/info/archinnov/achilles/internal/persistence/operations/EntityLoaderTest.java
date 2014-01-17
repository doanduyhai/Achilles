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
package info.archinnov.achilles.internal.persistence.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Row;

@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest {

	@InjectMocks
	private EntityLoader loader;

	@Mock
	private EntityMapper mapper;

	@Mock
	private CounterLoader counterLoader;

	@Mock
	private PersistenceContext context;

	@Mock
	private EntityMeta meta;

	@Mock
	private PropertyMeta idMeta;

	@Mock
	private PropertyMeta pm;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = new CompleteBean();

	@Before
	public void setUp() throws Exception {

		when(context.getEntity()).thenReturn(entity);
		when(context.getEntityMeta()).thenReturn(meta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(meta.getIdMeta()).thenReturn(idMeta);
	}

	@Test
	public void should_create_empty_entity() throws Exception {
		when(meta.instanciate()).thenReturn(entity);

		CompleteBean actual = loader.createEmptyEntity(context, CompleteBean.class);

		assertThat(actual).isSameAs(entity);

		verify(idMeta).setValueToField(actual, primaryKey);
	}

	@Test
	public void should_load_simple_entity() throws Exception {
		// Given
		Row row = mock(Row.class);
		when(meta.isClusteredCounter()).thenReturn(false);
		when(context.loadEntity()).thenReturn(row);
		when(meta.instanciate()).thenReturn(entity);

		// When
		CompleteBean actual = loader.load(context, CompleteBean.class);

		// Then
		assertThat(actual).isSameAs(entity);

		verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);
		verifyZeroInteractions(counterLoader);
	}

	@Test
	public void should_not_load_simple_entity_when_not_found() throws Exception {
		// Given
		when(meta.isClusteredCounter()).thenReturn(false);
		when(meta.instanciate()).thenReturn(entity);

		// When
		CompleteBean actual = loader.load(context, CompleteBean.class);

		// Then
		assertThat(actual).isNull();

		verifyZeroInteractions(mapper, counterLoader);
	}

	@Test
	public void should_load_clustered_counter_entity() throws Exception {
		// Given
		when(meta.isClusteredCounter()).thenReturn(true);
		when(counterLoader.loadClusteredCounters(context)).thenReturn(entity);

		// When
		CompleteBean actual = loader.load(context, CompleteBean.class);

		// Then
		assertThat(actual).isSameAs(entity);

		verifyZeroInteractions(mapper);
	}

	@Test
	public void should_load_properties_into_object() throws Exception {
		// Given
		when(pm.type()).thenReturn(PropertyType.SIMPLE);
		Row row = mock(Row.class);
		when(context.loadProperty(pm)).thenReturn(row);

		// When
		loader.loadPropertyIntoObject(context, entity, pm);

		// Then
		verify(mapper).setPropertyToEntity(row, pm, entity);
		verifyZeroInteractions(counterLoader);
	}

	@Test
	public void should_load_counter_properties_into_object() throws Exception {
		// Given
		when(pm.type()).thenReturn(PropertyType.COUNTER);

		// When
		loader.loadPropertyIntoObject(context, entity, pm);

		// Then
		verify(counterLoader).loadCounter(context, entity, pm);
		verifyZeroInteractions(mapper);
	}
}
