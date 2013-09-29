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
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.ThriftPersisterImpl;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private ThriftPersistenceContext context;

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();

	@Before
	public void setUp() {
		entityMeta = new EntityMeta();
		entityMeta.setTableName("cf");
		entityMeta.setClusteredEntity(false);

		entityDaosMap.clear();
		context = ThriftPersistenceContextTestBuilder
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId()).entity(entity)
				.entityDaosMap(entityDaosMap).build();
	}

	@Test
	public void should_persist_simple_property() throws Exception {
		PropertyMeta simpleMeta = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("simpleMeta", simpleMeta));

		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistSimpleProperty(context, simpleMeta);
	}

	@Test
	public void should_persist_partition_key_property() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("idMeta", idMeta));

		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistSimpleProperty(context, idMeta);
	}

	@Test
	public void should_persist_composite_partition_key_property() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(EMBEDDED_ID).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("idMeta", idMeta));

		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistSimpleProperty(context, idMeta);
	}

	@Test
	public void should_persist_counter_property() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.COUNTER).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("counterMeta", counterMeta));

		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).persistCounter(context, counterMeta);
	}

	@Test
	public void should_persist_list() throws Exception {
		ArrayList<String> list = new ArrayList<String>();

		PropertyMeta listMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends")
				.accessors().type(LIST).invoker(invoker).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("listMeta", listMeta));

		when(invoker.getValueFromField(entity, listMeta.getGetter())).thenReturn(list);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistList(list, context, listMeta);
	}

	@Test
	public void should_persist_set() throws Exception {
		Set<String> set = new HashSet<String>();

		PropertyMeta setMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("followers")
				.accessors().type(SET).invoker(invoker).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("setMeta", setMeta));

		when(invoker.getValueFromField(entity, setMeta.getGetter())).thenReturn(set);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistSet(set, context, setMeta);
	}

	@Test
	public void should_persist_map() throws Exception {
		Map<Integer, String> map = new HashMap<Integer, String>();

		PropertyMeta mapMeta = PropertyMetaTestBuilder.completeBean(Integer.class, String.class).field("preferences")
				.accessors().type(MAP).invoker(invoker).build();

		entityMeta.setPropertyMetas(ImmutableMap.of("mapMeta", mapMeta));

		when(invoker.getValueFromField(entity, mapMeta.getGetter())).thenReturn(map);
		persister.persist(context);

		verify(persisterImpl).removeEntityBatch(context);
		verify(persisterImpl).batchPersistMap(map, context, mapMeta);
	}

	@Test
	public void should_persist_clustered_entity() throws Exception {
		Object partitionKey = 10L;
		Object clusteredValue = "clusteredValue";

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.invoker(invoker).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));

		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(partitionKey);
		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(clusteredValue);

		persister.persist(context);

		verify(persisterImpl).persistClusteredEntity(context, clusteredValue);
	}

	@Test
	public void should_persist_value_less_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.invoker(invoker).build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta));

		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(partitionKey);

		persister.persist(context);

		verify(persisterImpl).persistClusteredEntity(context, "");

	}

	@Test
	public void should_persist_clustered_value() throws Exception {
		Object clusteredValue = "clusteredValue";
		Object partitionKey = RandomUtils.nextLong();

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).invoker(invoker)
				.build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("pm", pm));

		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(partitionKey);
		persister.persistClusteredValue(context, clusteredValue);

		verify(persisterImpl).persistClusteredValueBatch(context, clusteredValue);
	}

	@Test
	public void should_remove() throws Exception {
		persister.remove(context);
		verify(persisterImpl).remove(context);
	}

	@Test
	public void should_remove_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).invoker(invoker)
				.build();

		entityMeta.setClusteredEntity(true);
		entityMeta.setIdMeta(idMeta);
		when(invoker.getPartitionKey(entity.getId(), idMeta)).thenReturn(partitionKey);

		persister.remove(context);

		verify(persisterImpl).removeClusteredEntity(context);
	}

	@Test
	public void should_remove_property_as_batch() throws Exception {
		PropertyMeta nameMeta = new PropertyMeta();

		persister.removePropertyBatch(context, nameMeta);
		verify(persisterImpl).removePropertyBatch(context, nameMeta);
	}

}
