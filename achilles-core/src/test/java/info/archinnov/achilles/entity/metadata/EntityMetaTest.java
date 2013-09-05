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
package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class EntityMetaTest {
	@Test
	public void should_to_string() throws Exception {
		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("name", null);
		propertyMetas.put("age", null);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE)
				.consistencyLevels(Pair.create(ALL, ALL)).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClassName("className");
		entityMeta.setTableName("cfName");
		entityMeta.setIdClass(Long.class);
		entityMeta.setPropertyMetas(propertyMetas);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setClusteredEntity(true);
		entityMeta.setConsistencyLevels(Pair.create(ONE, ONE));

		StringBuilder toString = new StringBuilder();
		toString.append("EntityMeta [className=className, ");
		toString.append("columnFamilyName=cfName, ");
		toString.append("propertyMetas=[age,name], ");
		toString.append("idMeta=").append(idMeta.toString()).append(", ");
		toString.append("clusteredEntity=true, ");
		toString.append("consistencyLevels=[ONE,ONE]]");
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
	public void should_return_false_for_is_clustered_counter_if_not_clustered()
			throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta counterMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("count")
				.type(COUNTER).build();

		entityMeta.setClusteredEntity(false);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of(
				"count", counterMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_more_than_one_property()
			throws Exception {
		EntityMeta entityMeta = new EntityMeta();

		PropertyMeta nameMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).build();

		PropertyMeta counterMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("count")
				.type(COUNTER).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of(
				"name", nameMeta, "count", counterMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_value_less()
			throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		entityMeta.setClusteredEntity(false);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of(
				"idMeta", idMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_false_for_is_clustered_counter_if_not_counter_type()
			throws Exception {
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).build();
		entityMeta.setClusteredEntity(true);
		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of(
				"idMeta", idMeta, "nameMeta", nameMeta));

		assertThat(entityMeta.isClusteredCounter()).isFalse();
	}

	@Test
	public void should_return_null_when_no_first_meta() throws Exception {
		EntityMeta entityMeta = new EntityMeta();

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of(
				"idMeta", idMeta));

		assertThat(entityMeta.getFirstMeta()).isNull();
	}

	@Test
	public void should_return_true_when_value_less() throws Exception {
		EntityMeta entityMeta = new EntityMeta();

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of(
				"idMeta", idMeta));

		assertThat(entityMeta.isValueless()).isTrue();
	}
}
