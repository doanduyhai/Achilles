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
package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.COMPOSITE_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.Counter;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ThriftDaoFactoryTest {

	@InjectMocks
	private ThriftDaoFactory factory;

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private ThriftConsistencyLevelPolicy consistencyPolicy;

	private ConfigurationContext configContext = new ConfigurationContext();

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
	private Map<String, ThriftGenericWideRowDao> wideRowDaosMap = new HashMap<String, ThriftGenericWideRowDao>();

	@Before
	public void setUp() {
		configContext.setConsistencyPolicy(consistencyPolicy);
		entityDaosMap.clear();
		wideRowDaosMap.clear();
	}

	@Test
	public void should_create_counter_dao() throws Exception {
		ThriftCounterDao thriftCounterDao = factory.createCounterDao(cluster,
				keyspace, configContext);

		assertThat(thriftCounterDao).isNotNull();
		assertThat(Whitebox.getInternalState(thriftCounterDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(thriftCounterDao, "cluster"))
				.isSameAs(cluster);
		assertThat(Whitebox.getInternalState(thriftCounterDao, "keyspace"))
				.isSameAs(keyspace);
		assertThat(
				Whitebox.getInternalState(thriftCounterDao,
						"columnNameSerializer")).isSameAs(COMPOSITE_SRZ);
		Pair<Class<Composite>, Class<Long>> rowAndValueClases = Whitebox
				.getInternalState(thriftCounterDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClases.left).isSameAs(Composite.class);
		assertThat(rowAndValueClases.right).isSameAs(Long.class);
	}

	@Test
	public void should_create_entity_dao() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class).field("id").build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClusteredEntity(false);
		entityMeta.setTableName("cf");
		entityMeta.setIdMeta(idMeta);
		entityMeta.setIdClass(Long.class);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta>());

		factory.createDaosForEntity(cluster, keyspace, configContext,
				entityMeta, entityDaosMap, wideRowDaosMap);

		ThriftGenericEntityDao entityDao = entityDaosMap.get("cf");

		assertThat(entityDao).isNotNull();
		assertThat(entityDao.getColumnFamily()).isEqualTo("cf");
		assertThat(Whitebox.getInternalState(entityDao, "policy")).isSameAs(
				consistencyPolicy);
		assertThat(Whitebox.getInternalState(entityDao, "cluster")).isSameAs(
				cluster);
		assertThat(Whitebox.getInternalState(entityDao, "keyspace")).isSameAs(
				keyspace);
		assertThat(Whitebox.getInternalState(entityDao, "columnNameSerializer"))
				.isSameAs(COMPOSITE_SRZ);

		Pair<Class<Long>, Class<String>> rowAndValueClases = Whitebox
				.getInternalState(entityDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClases.left).isSameAs(Long.class);
		assertThat(rowAndValueClases.right).isSameAs(String.class);
	}

	// Clustered Entity Dao
	@Test
	public void should_create_clustered_entity_dao() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Integer.class, String.class, UUID.class)
				.field("id").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Date.class)
				.type(PropertyType.SIMPLE).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setTableName("cf");
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		factory.createClusteredEntityDao(cluster, keyspace, configContext,
				entityMeta, wideRowDaosMap);

		ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

		assertThat(clusteredEntityDao).isNotNull();
		assertThat(clusteredEntityDao.getColumnFamily()).isEqualTo("cf");
		assertThat(Whitebox.getInternalState(clusteredEntityDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(clusteredEntityDao, "cluster"))
				.isSameAs(cluster);
		assertThat(Whitebox.getInternalState(clusteredEntityDao, "keyspace"))
				.isSameAs(keyspace);
		assertThat(
				Whitebox.getInternalState(clusteredEntityDao,
						"columnNameSerializer")).isSameAs(COMPOSITE_SRZ);

		Pair<Class<Integer>, Class<Date>> rowAndValueClasses = Whitebox
				.getInternalState(clusteredEntityDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClasses.left).isSameAs(Integer.class);
		assertThat(rowAndValueClasses.right).isSameAs(Date.class);
	}

	@Test
	public void should_create_counter_clustered_entity_dao() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Integer.class, String.class, UUID.class)
				.field("id").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Counter.class)
				.type(PropertyType.COUNTER).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setTableName("cf");
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		factory.createClusteredEntityDao(cluster, keyspace, configContext,
				entityMeta, wideRowDaosMap);

		ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

		Pair<Class<Integer>, Class<Long>> rowAndValueClases = Whitebox
				.getInternalState(clusteredEntityDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClases.left).isSameAs(Integer.class);
		assertThat(rowAndValueClases.right).isSameAs(Long.class);
	}

	@Test
	public void should_create_object_type_clustered_entity_dao()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Integer.class, String.class, UUID.class)
				.field("id").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(PropertyType.SIMPLE).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setTableName("cf");
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		factory.createClusteredEntityDao(cluster, keyspace, configContext,
				entityMeta, wideRowDaosMap);

		ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

		Pair<Class<Integer>, Class<String>> rowAndValueClases = Whitebox
				.getInternalState(clusteredEntityDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClases.left).isSameAs(Integer.class);
		assertThat(rowAndValueClases.right).isSameAs(String.class);
	}

	@Test
	public void should_create_join_clustered_entity_dao() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Integer.class, String.class, UUID.class)
				.field("id").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta joinIdMeta = PropertyMetaTestBuilder
				.valueClass(UUID.class).build();

		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(PropertyType.JOIN_SIMPLE).joinMeta(joinMeta).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setTableName("cf");
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		factory.createClusteredEntityDao(cluster, keyspace, configContext,
				entityMeta, wideRowDaosMap);

		ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

		Pair<Class<Integer>, Class<UUID>> rowAndValueClases = Whitebox
				.getInternalState(clusteredEntityDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClases.left).isSameAs(Integer.class);
		assertThat(rowAndValueClases.right).isSameAs(UUID.class);
	}

	@Test
	public void should_create_value_less_clustered_entity_dao()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Integer.class, String.class, UUID.class)
				.field("id").type(PropertyType.EMBEDDED_ID).build();

		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setTableName("cf");
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta));

		factory.createClusteredEntityDao(cluster, keyspace, configContext,
				entityMeta, wideRowDaosMap);

		ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

		assertThat(clusteredEntityDao).isNotNull();
		assertThat(clusteredEntityDao.getColumnFamily()).isEqualTo("cf");
		assertThat(Whitebox.getInternalState(clusteredEntityDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(clusteredEntityDao, "cluster"))
				.isSameAs(cluster);
		assertThat(Whitebox.getInternalState(clusteredEntityDao, "keyspace"))
				.isSameAs(keyspace);
		assertThat(
				Whitebox.getInternalState(clusteredEntityDao,
						"columnNameSerializer")).isSameAs(COMPOSITE_SRZ);

		Pair<Class<Integer>, Class<String>> rowAndValueClasses = Whitebox
				.getInternalState(clusteredEntityDao, "rowkeyAndValueClasses");
		assertThat(rowAndValueClasses.left).isSameAs(Integer.class);
		assertThat(rowAndValueClasses.right).isSameAs(String.class);
	}
}
