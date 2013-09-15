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

import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.ThriftPersisterImpl;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityPersisterTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityPersister persister;

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private ThriftPersisterImpl persisterImpl;

	@Mock
	private ReflectionInvoker invoker;

	private EntityMeta entityMeta;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
			.buid();

	private ThriftPersistenceContext context;

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();

	@Before
	public void setUp() {
		entityMeta = new EntityMeta();
		entityMeta.setTableName("cf");
		entityMeta.setClusteredEntity(false);

		entityDaosMap.clear();
		context = ThriftPersistenceContextTestBuilder
				.context(entityMeta, thriftCounterDao, policy,
						CompleteBean.class, entity.getId()).entity(entity)
				.entityDaosMap(entityDaosMap).build();
	}

	@Test
	public void should_persist_simple_property() throws Exception {
		PropertyMeta simpleMeta = PropertyMetaTestBuilder
				.valueClass(String.class).type(PropertyType.SIMPLE).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("simpleMeta", simpleMeta));

		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistSimpleProperty(context, simpleMeta);
	}

	@Test
	public void should_persist_counter_property() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder
				.valueClass(Long.class).type(PropertyType.COUNTER).build();

		entityMeta
				.setPropertyMetas(ImmutableMap.of("counterMeta", counterMeta));

		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).persistCounter(context, counterMeta);
	}

	@Test
	public void should_not_persist_twice_the_same_entity() throws Exception {
		PropertyMeta simpleMeta = PropertyMetaTestBuilder
				.valueClass(String.class).type(PropertyType.SIMPLE).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("simpleMeta", simpleMeta));

		persister.persist(context);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl, times(1)).batchPersistSimpleProperty(context,
				simpleMeta);
	}

	@Test
	public void should_cascade_persist() throws Exception {
		PropertyMeta joinIdMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();
		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, UserBean.class).field("user")
				.accessors().type(PropertyType.JOIN_SIMPLE).joinMeta(joinMeta)
				.cascadeType(CascadeType.PERSIST)
				.consistencyLevels(Pair.create(ALL, ALL)).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("propertyMeta",
				propertyMeta));

		Long joinId = RandomUtils.nextLong();

		UserBean user = new UserBean();

		when(invoker.getPrimaryKey(entity, joinIdMeta)).thenReturn(joinId);
		when(invoker.getValueFromField(entity, propertyMeta.getGetter()))
				.thenReturn(user);

		persister.cascadePersistOrEnsureExists(context, entity,
				propertyMeta.getJoinProperties());

		verify(persisterImpl).batchPersistJoinEntity(context, propertyMeta,
				user, persister);
	}

	@Test
	public void should_ensure_join_entity_exist() throws Exception {
		PropertyMeta joinIdMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();
		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setTableName("cfName");
		ThriftGenericEntityDao entityDao = mock(ThriftGenericEntityDao.class);
		entityDaosMap.put("cfName", entityDao);
		Long joinId = RandomUtils.nextLong();
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		when(invoker.getPrimaryKey(entity, joinIdMeta)).thenReturn(joinId);
		when(loader.loadPrimaryKey(context, joinIdMeta)).thenReturn(joinId);
		context.getConfigContext().setEnsureJoinConsistency(true);

		Object actual = persister.cascadePersistOrEnsureExists(context, entity,
				joinProperties);
		assertThat(actual).isEqualTo(joinId);

		verify(loader).loadPrimaryKey(context, joinIdMeta);
	}

	@Test
	public void should_not_ensure_join_entity_exist_if_join_consistency_flag_false()
			throws Exception {
		PropertyMeta joinIdMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();
		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setTableName("cfName");
		ThriftGenericEntityDao entityDao = mock(ThriftGenericEntityDao.class);
		entityDaosMap.put("cfName", entityDao);
		Long joinId = RandomUtils.nextLong();
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		when(invoker.getPrimaryKey(entity, joinIdMeta)).thenReturn(joinId);
		when(loader.loadPrimaryKey(context, joinIdMeta)).thenReturn(joinId);
		context.getConfigContext().setEnsureJoinConsistency(false);

		Object actual = persister.cascadePersistOrEnsureExists(context, entity,
				joinProperties);

		assertThat(actual).isEqualTo(joinId);

		verifyZeroInteractions(loader);

	}

	@Test
	public void should_not_ensure_join_entity_exist_if_already_processed()
			throws Exception {
		PropertyMeta joinIdMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.SIMPLE).build();
		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setTableName("cfName");
		ThriftGenericEntityDao entityDao = mock(ThriftGenericEntityDao.class);
		entityDaosMap.put("cfName", entityDao);
		Long joinId = RandomUtils.nextLong();
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		when(invoker.getPrimaryKey(entity, joinIdMeta)).thenReturn(joinId);
		when(loader.loadPrimaryKey(context, joinIdMeta)).thenReturn(joinId);
		context.getConfigContext().setEnsureJoinConsistency(true);
		context.addToProcessingList(entity);

		Object actual = persister.cascadePersistOrEnsureExists(context, entity,
				joinProperties);

		assertThat(actual).isEqualTo(joinId);

		verifyZeroInteractions(loader);

	}

	@Test
	public void should_persist_list() throws Exception {
		ArrayList<String> list = new ArrayList<String>();

		PropertyMeta listMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, String.class).field("friends")
				.accessors().type(PropertyType.LIST).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("listMeta", listMeta));

		when(invoker.getValueFromField(entity, listMeta.getGetter()))
				.thenReturn(list);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistList(list, context, listMeta);
	}

	@Test
	public void should_persist_set() throws Exception {
		Set<String> set = new HashSet<String>();

		PropertyMeta setMeta = PropertyMetaTestBuilder
				//
				.completeBean(Void.class, String.class).field("followers")
				.accessors().type(PropertyType.SET).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("setMeta", setMeta));

		when(invoker.getValueFromField(entity, setMeta.getGetter()))
				.thenReturn(set);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistSet(set, context, setMeta);
	}

	@Test
	public void should_persist_map() throws Exception {
		Map<Integer, String> map = new HashMap<Integer, String>();

		PropertyMeta mapMeta = PropertyMetaTestBuilder
				//
				.completeBean(Integer.class, String.class).field("preferences")
				.accessors().type(PropertyType.MAP).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("mapMeta", mapMeta));

		when(invoker.getValueFromField(entity, mapMeta.getGetter()))
				.thenReturn(map);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistMap(map, context, mapMeta);
	}

	@Test
	public void should_persist_join() throws Exception {
		UserBean user = new UserBean();

		PropertyMeta joinMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.accessors().type(PropertyType.JOIN_SIMPLE).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("joinMeta", joinMeta));

		when(invoker.getValueFromField(entity, joinMeta.getGetter()))
				.thenReturn(user);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistJoinEntity(context, joinMeta, user,
				persister);
	}

	@Test
	public void should_persist_join_list() throws Exception {
		List<String> joinList = new ArrayList<String>();

		PropertyMeta joinListMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("friends")
				.accessors().type(PropertyType.JOIN_LIST).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("joinListMeta",
				joinListMeta));

		when(invoker.getValueFromField(entity, joinListMeta.getGetter()))
				.thenReturn(joinList);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistJoinList(context, joinListMeta,
				joinList, persister);
	}

	@Test
	public void should_persist_join_set() throws Exception {
		Set<String> joinSet = new HashSet<String>();

		PropertyMeta joinSetMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("followers")
				.accessors().type(PropertyType.JOIN_SET).build();

		entityMeta
				.setPropertyMetas(ImmutableMap.of("joinSetMeta", joinSetMeta));

		when(invoker.getValueFromField(entity, joinSetMeta.getGetter()))
				.thenReturn(joinSet);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistJoinSet(context, joinSetMeta,
				joinSet, persister);
	}

	@Test
	public void should_persist_join_map() throws Exception {
		Map<Integer, String> joinMap = new HashMap<Integer, String>();

		PropertyMeta joinMapMeta = PropertyMetaTestBuilder
				//
				.completeBean(Integer.class, String.class).field("preferences")
				.accessors().type(PropertyType.JOIN_MAP).build();

		entityMeta
				.setPropertyMetas(ImmutableMap.of("joinMapMeta", joinMapMeta));

		when(invoker.getValueFromField(entity, joinMapMeta.getGetter()))
				.thenReturn(joinMap);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistJoinMap(context, joinMapMeta,
				joinMap, persister);
	}

	@Test
	public void should_persist_clustered_entity() throws Exception {
		Object partitionKey = 10L;
		Object clusteredValue = "clusteredValue";

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).field("id")
				.type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.SIMPLE).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(
				partitionKey);
		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(
				clusteredValue);

		persister.persist(context);

		verify(persisterImpl).persistClusteredEntity(persister, context,
				partitionKey, clusteredValue);
	}

	@Test
	public void should_persist_value_less_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).field("id")
				.type(PropertyType.EMBEDDED_ID).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta));

		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(
				partitionKey);

		persister.persist(context);

		verify(persisterImpl).persistClusteredEntity(persister, context,
				partitionKey, "");

	}

	@Test
	public void should_persist_clustered_value() throws Exception {
		Object clusteredValue = "clusteredValue";
		Object partitionKey = RandomUtils.nextLong();

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.SIMPLE).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("pm", pm));

		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(
				partitionKey);
		persister.persistClusteredValue(context, clusteredValue);

		verify(persisterImpl).persistClusteredValueBatch(context, partitionKey,
				clusteredValue, persister);
	}

	@Test
	public void should_remove() throws Exception {
		persister.remove(context);
		verify(persisterImpl).remove(context);
	}

	@Test
	public void should_remove_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(
				partitionKey);

		persister.remove(context);

		verify(persisterImpl).removeClusteredEntity(context, partitionKey);
	}

	@Test
	public void should_remove_property_as_batch() throws Exception {
		PropertyMeta nameMeta = new PropertyMeta();

		persister.removePropertyBatch(context, nameMeta);
		verify(persisterImpl).removePropertyBatch(context, nameMeta);
	}

}
