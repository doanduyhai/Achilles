/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.interceptor.Event.*;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fest.assertions.api.Assertions;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class EntityMetaTest {
	@Test
	public void should_to_string() throws Exception {
		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("name", null);
		propertyMetas.put("age", null);

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).consistencyLevels(Pair.create(ALL, ALL)).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClassName("className");
		entityMeta.setTableName("cfName");
		entityMeta.setIdClass(Long.class);
		entityMeta.setPropertyMetas(propertyMetas);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setClusteredEntity(true);
		entityMeta.setConsistencyLevels(Pair.create(ONE, ONE));

		StringBuilder toString = new StringBuilder();
		toString.append("EntityMeta{className=className, ");
		toString.append("tableName/columnFamilyName=cfName, ");
		toString.append("propertyMetas=age,name, ");
		toString.append("idMeta=").append(idMeta.toString()).append(", ");
		toString.append("clusteredEntity=true, ");
		toString.append("consistencyLevels=(ONE,ONE)}");
		assertThat(entityMeta.toString()).isEqualTo(toString.toString());
	}

	@Test
	public void should_get_all_metas() throws Exception {

		PropertyMeta pm1 = new PropertyMeta();
		PropertyMeta pm2 = new PropertyMeta();

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("name", pm1);
		propertyMetas.put("age", pm2);

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(propertyMetas);

		assertThat(entityMeta.getAllMetas()).containsExactly(pm1, pm2);
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_not_clustered() throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta counterMeta = PropertyMetaTestBuilder
		//
				.completeBean(Void.class, Long.class).field("count").type(COUNTER).build();

		entityMeta.setClusteredEntity(false);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of("count", counterMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_more_than_one_property() throws Exception {
		EntityMeta entityMeta = new EntityMeta();

		PropertyMeta nameMeta = PropertyMetaTestBuilder
		//
				.completeBean(Void.class, String.class).field("name").type(SIMPLE).build();

		PropertyMeta counterMeta = PropertyMetaTestBuilder
		//
				.completeBean(Void.class, Long.class).field("count").type(COUNTER).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of("name", nameMeta, "count", counterMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_value_less() throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		entityMeta.setClusteredEntity(false);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of("idMeta", idMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_not_counter_type() throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder
		//
				.completeBean(Void.class, String.class).field("name").type(SIMPLE).build();
		entityMeta.setClusteredEntity(true);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of("idMeta", idMeta, "nameMeta", nameMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_true_when_value_less() throws Exception {
		EntityMeta entityMeta = new EntityMeta();

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of("idMeta", idMeta));

		assertThat(entityMeta.isValueless()).isTrue();
	}

	@Test
	public void should_return_true_when_has_embedded_id() throws Exception {
		PropertyMeta idMeta = new PropertyMeta();
		idMeta.setType(EMBEDDED_ID);
		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);

		assertThat(meta.hasEmbeddedId()).isTrue();

	}

	@Test
	public void should_return_event_interceptors_for_specific_event() throws Exception {
		// Given
		EntityMeta entityMeta = new EntityMeta();
		Interceptor<String> postPersistInterceptor = createInterceptor(POST_PERSIST);
		Interceptor<String> prePersistInterceptor = createInterceptor(PRE_PERSIST);

		// When
		entityMeta.addInterceptor(postPersistInterceptor);
		entityMeta.addInterceptor(prePersistInterceptor);

		// Then
		assertThat(entityMeta.getInterceptorsForEvent(POST_PERSIST)).containsExactly(postPersistInterceptor);
		assertThat(entityMeta.getInterceptorsForEvent(PRE_PERSIST)).containsExactly(prePersistInterceptor);
	}

	@Test
	public void should_apply_right_interceptor_on_right_event() throws Exception {

		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.EMBEDDED_ID).accessors().build();
		idMeta.setInvoker(new ReflectionInvoker());
		entityMeta.setIdMeta(idMeta);
		entityMeta.addInterceptor(createInterceptorForCompleteBean(PRE_PERSIST, 30L));
		entityMeta.addInterceptor(createInterceptorForCompleteBean(POST_PERSIST, 35L));

		entityMeta.intercept(bean, PRE_PERSIST);
		Assertions.assertThat(bean.getAge()).isEqualTo(30L);
		entityMeta.intercept(bean, POST_PERSIST);
		Assertions.assertThat(bean.getAge()).isEqualTo(35L);
	}

	private Interceptor<String> createInterceptor(final Event event) {
		Interceptor<String> interceptor = new Interceptor<String>() {

			@Override
			public void onEvent(String entity) {
			}

			@Override
			public List<Event> events() {
				List<Event> events = new ArrayList<>();
				events.add(event);
				return events;
			}
		};
		return interceptor;
	}

	private Interceptor<CompleteBean> createInterceptorForCompleteBean(final Event event, final long age) {
		Interceptor<CompleteBean> interceptor = new Interceptor<CompleteBean>() {

			@Override
			public void onEvent(CompleteBean entity) {
				entity.setAge(age);
			}

			@Override
			public List<Event> events() {
				return Arrays.asList(event);
			}
		};
		return interceptor;
	}
}
