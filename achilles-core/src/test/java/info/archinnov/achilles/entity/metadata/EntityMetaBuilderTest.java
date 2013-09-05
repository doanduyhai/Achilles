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

import static info.archinnov.achilles.entity.metadata.EntityMetaBuilder.entityMetaBuilder;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.Bean;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaBuilderTest {

	@Mock
	private PropertyMeta idMeta;

	@Test
	public void should_build_meta() throws Exception {

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		PropertyMeta simpleMeta = new PropertyMeta();
		simpleMeta.setType(SIMPLE);

		Method getter = Bean.class.getDeclaredMethod("getName",
				(Class<?>[]) null);
		simpleMeta.setGetter(getter);

		Method setter = Bean.class.getDeclaredMethod("setName", String.class);
		simpleMeta.setSetter(setter);

		propertyMetas.put("name", simpleMeta);

		when((Class) idMeta.getValueClass()).thenReturn(Long.class);

		List<PropertyMeta> eagerMetas = new ArrayList<PropertyMeta>();
		eagerMetas.add(simpleMeta);

		EntityMeta meta = entityMetaBuilder(idMeta)
				.entityClass(CompleteBean.class).className("Bean")
				.columnFamilyName("cfName").propertyMetas(propertyMetas)
				.build();

		assertThat((Class) meta.getEntityClass()).isEqualTo(CompleteBean.class);
		assertThat(meta.getClassName()).isEqualTo("Bean");
		assertThat(meta.getTableName()).isEqualTo("cfName");
		assertThat((PropertyMeta) meta.getIdMeta()).isSameAs(idMeta);
		assertThat((Class<Long>) meta.getIdClass()).isEqualTo(Long.class);
		assertThat(meta.getPropertyMetas()).containsKey("name");
		assertThat(meta.getPropertyMetas()).containsValue(simpleMeta);

		assertThat(meta.getGetterMetas()).hasSize(1);
		assertThat(meta.getGetterMetas().containsKey(getter));
		assertThat(meta.getGetterMetas().get(getter)).isSameAs(
				(PropertyMeta) simpleMeta);

		assertThat(meta.getSetterMetas()).hasSize(1);
		assertThat(meta.getSetterMetas().containsKey(setter));
		assertThat(meta.getSetterMetas().get(setter)).isSameAs(
				(PropertyMeta) simpleMeta);

		assertThat(meta.getEagerMetas()).containsOnly(simpleMeta);
		assertThat(meta.getEagerGetters()).containsOnly(simpleMeta.getGetter());
		assertThat(meta.getAllMetasExceptIdMeta()).containsOnly(simpleMeta);
		assertThat(meta.getFirstMeta()).isSameAs((PropertyMeta) simpleMeta);
	}

	@Test
	public void should_build_meta_with_column_family_name() throws Exception {

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		PropertyMeta simpleMeta = new PropertyMeta();
		simpleMeta.setType(SIMPLE);
		propertyMetas.put("name", simpleMeta);

		when((Class) idMeta.getValueClass()).thenReturn(Long.class);

		List<PropertyMeta> eagerMetas = new ArrayList<PropertyMeta>();
		eagerMetas.add(simpleMeta);

		EntityMeta meta = entityMetaBuilder(idMeta).className("Bean")
				.propertyMetas(propertyMetas).columnFamilyName("toto").build();

		assertThat(meta.getClassName()).isEqualTo("Bean");
		assertThat(meta.getTableName()).isEqualTo("toto");
	}

	@Test
	public void should_build_meta_with_consistency_levels() throws Exception {
		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		PropertyMeta nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().build();
		propertyMetas.put("name", nameMeta);

		when((Class) idMeta.getValueClass()).thenReturn(Long.class);
		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = Pair
				.create(ConsistencyLevel.ONE, ConsistencyLevel.TWO);
		List<PropertyMeta> eagerMetas = new ArrayList<PropertyMeta>();

		EntityMeta meta = entityMetaBuilder(idMeta).className("Bean")
				.propertyMetas(propertyMetas).columnFamilyName("toto")
				.consistencyLevels(consistencyLevels).build();

		assertThat(meta.getConsistencyLevels()).isSameAs(consistencyLevels);
	}

	@Test
	public void should_build_clustered_counter_meta() throws Exception {

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		PropertyMeta counterMeta = new PropertyMeta();
		counterMeta.setType(COUNTER);

		propertyMetas.put("id", idMeta);
		propertyMetas.put("counter", counterMeta);

		when(idMeta.type()).thenReturn(EMBEDDED_ID);
		when((Class) idMeta.getValueClass()).thenReturn(Long.class);
		when(idMeta.isEmbeddedId()).thenReturn(true);

		List<PropertyMeta> eagerMetas = new ArrayList<PropertyMeta>();
		eagerMetas.add(counterMeta);

		EntityMeta meta = entityMetaBuilder(idMeta)
				.entityClass(CompleteBean.class).className("Bean")
				.columnFamilyName("cfName").propertyMetas(propertyMetas)
				.build();

		assertThat(meta.isClusteredEntity()).isTrue();
		assertThat(meta.isClusteredCounter()).isTrue();
	}

}
