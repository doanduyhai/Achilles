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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Counter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class ThriftPersisterImplTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftPersisterImpl persisterImpl;

	@Mock
	private ThriftEntityPersister persister;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private EntityProxifier<ThriftPersistenceContext> proxifier;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao wideRowDao;

	@Mock
	private ThriftCounterDao counterDao;

	private EntityMeta entityMeta;

	@Mock
	private ThriftCompositeFactory compositeFactory;

	@Mock
	private Mutator<Object> entityMutator;

	@Mock
	private Mutator<Object> wideRowMutator;

	@Mock
	private Mutator<Object> counterMutator;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();

	private Map<String, ThriftGenericWideRowDao> wideRowDaosMap = new HashMap<String, ThriftGenericWideRowDao>();

	@Mock
	private ThriftImmediateFlushContext flushContext;

	@Mock
	private DataTranscoder transcoder;

	private Optional<Integer> ttlO = Optional.<Integer> absent();

	private Optional<Long> timestampO = Optional.<Long> absent();

	@Captor
	private ArgumentCaptor<Composite> compositeCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private ThriftPersistenceContext context;

	@Before
	public void setUp() {
		entityMeta = new EntityMeta();
		entityMeta.setTableName("cf");
		context = ThriftPersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId()).entity(entity)
				.thriftImmediateFlushContext(flushContext).entityDao(entityDao).wideRowDao(wideRowDao)
				.wideRowDaosMap(wideRowDaosMap).entityDaosMap(entityDaosMap).build();
		when(flushContext.getEntityMutator("cf")).thenReturn(entityMutator);
		when(flushContext.getConsistencyLevel()).thenReturn(EACH_QUORUM);
		when(compositeFactory.buildRowKey(context)).thenReturn(entity.getId());
		entityDaosMap.clear();
		wideRowDaosMap.clear();
	}

	@Test
	public void should_persist_clustered_entity() throws Exception {
		Object partitionKey = 10L;
		String clusteredValue = "clusteredValue";

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Integer.class).type(SIMPLE).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		Composite comp = new Composite();

		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(flushContext.getWideRowMutator("cf")).thenReturn(wideRowMutator);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.persistClusteredEntity(context, clusteredValue);
		verify(wideRowDao).setValueBatch(partitionKey, comp, clusteredValue, ttlO, timestampO, wideRowMutator);
	}

	@Test
	public void should_persist_counter_clustered_entity() throws Exception {
		Object partitionKey = 10L;
		Counter clusteredValue = CounterBuilder.incr(10L);

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Long.class).type(COUNTER).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		Composite comp = new Composite();
		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.persistClusteredEntity(context, clusteredValue);

		verify(wideRowDao).incrementCounter(partitionKey, comp, 10L);
	}

	@Test
	public void should_persist_value_less_clustered_entity() throws Exception {
		Object partitionKey = 10L;
		String clusteredValue = "";

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta));

		Composite comp = new Composite();
		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(flushContext.getWideRowMutator("cf")).thenReturn(wideRowMutator);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.persistClusteredEntity(context, clusteredValue);

		verify(wideRowDao).setValueBatch(partitionKey, comp, clusteredValue, ttlO, timestampO, wideRowMutator);
	}

	@Test
	public void should_persist_simple_clustered_value_batch() throws Exception {
		Object partitionKey = 10L;
		String clusteredValue = "clusteredValue";

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		Composite comp = new Composite();
		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(flushContext.getWideRowMutator("cf")).thenReturn(wideRowMutator);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.persistClusteredValueBatch(context, clusteredValue);

		verify(wideRowDao).setValueBatch(partitionKey, comp, clusteredValue, ttlO, timestampO, wideRowMutator);
	}

	@Test
	public void should_batch_simple_property() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.transcoder(transcoder).accessors().invoker(invoker).build();

		Composite comp = new Composite();
		when(compositeFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(comp);

		when(invoker.getValueFromField(entity, propertyMeta.getGetter())).thenReturn("testValue");

		when(transcoder.forceEncodeToJSON("testValue")).thenReturn("testValue");
		persisterImpl.batchPersistSimpleProperty(context, propertyMeta);

		verify(entityDao).insertColumnBatch(entity.getId(), comp, "testValue", ttlO, timestampO, entityMutator);

	}

	@Test
	public void should_persist_counter_property() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID)
				.invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);

		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.fqcn("fqcn").accessors().invoker(invoker).build();

		Counter counterValue = CounterBuilder.incr(10L);
		when(invoker.getValueFromField(entity, propertyMeta.getGetter())).thenReturn(counterValue);

		Composite rowKey = new Composite();
		Composite name = new Composite();
		when(compositeFactory.createRowKeyForCounter("fqcn", entity.getId(), idMeta)).thenReturn(rowKey);
		when(compositeFactory.createBaseForCounterGet(propertyMeta)).thenReturn(name);

		persisterImpl.persistCounter(context, propertyMeta);

		verify(counterDao).incrementCounter(rowKey, name, 10L);

	}

	@Test
	public void should_batch_list_property() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends")
				.transcoder(transcoder).accessors().invoker(invoker).build();

		Composite comp1 = new Composite();
		Composite comp2 = new Composite();
		when(compositeFactory.createForBatchInsertList(propertyMeta, 0)).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertList(propertyMeta, 1)).thenReturn(comp2);

		when(transcoder.forceEncodeToJSON("foo")).thenReturn("foo");
		when(transcoder.forceEncodeToJSON("bar")).thenReturn("bar");

		persisterImpl.batchPersistList(Arrays.asList("foo", "bar"), context, propertyMeta);

		InOrder inOrder = inOrder(entityDao);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp1, "foo", ttlO, timestampO, entityMutator);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp2, "bar", ttlO, timestampO, entityMutator);

	}

	@Test
	public void should_batch_set_property() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("followers")
				.transcoder(transcoder).accessors().invoker(invoker).build();

		Composite comp1 = new Composite();
		comp1.setComponent(0, "John", STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		Composite comp2 = new Composite();
		comp2.setComponent(0, "Helen", STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());

		when(transcoder.forceEncodeToJSON("John")).thenReturn("John");
		when(transcoder.forceEncodeToJSON("Helen")).thenReturn("Helen");

		when(compositeFactory.createForBatchInsertSetOrMap(propertyMeta, "John")).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertSetOrMap(propertyMeta, "Helen")).thenReturn(comp2);

		Set<String> followers = ImmutableSet.of("John", "Helen");
		persisterImpl.batchPersistSet(followers, context, propertyMeta);

		InOrder inOrder = inOrder(entityDao);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp1, "", ttlO, timestampO, entityMutator);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp2, "", ttlO, timestampO, entityMutator);

	}

	@Test
	public void should_batch_map_property() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Integer.class, String.class)
				.field("preferences").transcoder(transcoder).type(MAP).accessors().invoker(invoker).build();

		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(transcoder.forceEncodeToJSON(1)).thenReturn("1");
		when(transcoder.forceEncodeToJSON("FR")).thenReturn("FR");
		when(transcoder.forceEncodeToJSON(2)).thenReturn("2");
		when(transcoder.forceEncodeToJSON("Paris")).thenReturn("Paris");
		when(transcoder.forceEncodeToJSON(3)).thenReturn("3");
		when(transcoder.forceEncodeToJSON("75014")).thenReturn("75014");

		Composite comp1 = new Composite();
		comp1.setComponent(0, "1", STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		Composite comp2 = new Composite();
		comp2.setComponent(0, "2", STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		Composite comp3 = new Composite();
		comp3.setComponent(0, "3", STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());

		when(compositeFactory.createForBatchInsertSetOrMap(propertyMeta, "1")).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertSetOrMap(propertyMeta, "2")).thenReturn(comp2);
		when(compositeFactory.createForBatchInsertSetOrMap(propertyMeta, "3")).thenReturn(comp3);

		persisterImpl.batchPersistMap(map, context, propertyMeta);

		ArgumentCaptor<Composite> compositeCaptor = ArgumentCaptor.forClass(Composite.class);
		ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

		verify(entityDao, times(3)).insertColumnBatch(eq(entity.getId()), compositeCaptor.capture(),
				valueCaptor.capture(), eq(ttlO), eq(timestampO), eq(entityMutator));

		assertThat(compositeCaptor.getAllValues()).hasSize(3);
		assertThat(valueCaptor.getAllValues()).hasSize(3);

		List<Composite> composites = compositeCaptor.getAllValues();
		List<String> values = valueCaptor.getAllValues();

		assertThat(composites.get(0)).isEqualTo(comp1);
		assertThat(values.get(0)).isEqualTo("FR");

		assertThat(composites.get(1)).isEqualTo(comp2);
		assertThat(values.get(1)).isEqualTo("Paris");

		assertThat(composites.get(2)).isEqualTo(comp3);
		assertThat(values.get(2)).isEqualTo("75014");
	}

	@Test
	public void should_remove_entity_having_simple_counter() throws Exception {
		String fqcn = CompleteBean.class.getCanonicalName();

		PropertyMeta counterIdMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.accessors().invoker(invoker).build();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, Counter.class).field("count")
				.type(PropertyType.COUNTER).accessors().counterIdMeta(counterIdMeta).fqcn(fqcn)
				.consistencyLevels(Pair.create(ONE, ALL)).invoker(invoker).build();

		entityMeta.setClusteredEntity(false);
		entityMeta.setPropertyMetas(ImmutableMap.of("pm", propertyMeta));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(propertyMeta));
		entityMeta.setFirstMeta(propertyMeta);

		Composite keyComp = new Composite();
		Composite comp = new Composite();
		when(compositeFactory.createRowKeyForCounter(fqcn, entity.getId(), counterIdMeta)).thenReturn(keyComp);
		when(compositeFactory.createForBatchInsertSingleCounter(propertyMeta)).thenReturn(comp);
		when(flushContext.getCounterMutator()).thenReturn(counterMutator);

		persisterImpl.remove(context);

		verify(counterDao).removeCounterBatch(keyComp, comp, counterMutator);

	}

	@Test
	public void should_remove_entity_batch() throws Exception {
		Object primaryKey = entity.getId();
		when(flushContext.getEntityMutator("cf")).thenReturn(entityMutator);

		persisterImpl.removeEntityBatch(context);

		verify(entityDao).removeRowBatch(primaryKey, entityMutator);
	}

	@Test
	public void should_remove_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.invoker(invoker).build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Integer.class).type(SIMPLE).invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		Composite comp = new Composite();

		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(flushContext.getWideRowMutator("cf")).thenReturn(wideRowMutator);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.removeClusteredEntity(context);
		verify(wideRowDao).removeColumnBatch(partitionKey, comp, wideRowMutator);
	}

	@Test
	public void should_remove_value_less_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta));

		Composite comp = new Composite();

		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(flushContext.getWideRowMutator("cf")).thenReturn(wideRowMutator);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.removeClusteredEntity(context);
		verify(wideRowDao).removeColumnBatch(partitionKey, comp, wideRowMutator);
	}

	@Test
	public void should_remove_counter_clustered_entity() throws Exception {
		Object partitionKey = 10L;

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).field("id").type(EMBEDDED_ID)
				.invoker(invoker).build();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Long.class).type(COUNTER).invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));
		entityMeta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		entityMeta.setFirstMeta(pm);

		Composite comp = new Composite();

		when(compositeFactory.createCompositeForClusteringComponents(context)).thenReturn(comp);
		wideRowDaosMap.put("cf", wideRowDao);
		when(flushContext.getWideRowMutator("cf")).thenReturn(wideRowMutator);
		when(compositeFactory.buildRowKey(context)).thenReturn(partitionKey);

		persisterImpl.removeClusteredEntity(context);
		verify(wideRowDao).removeCounterBatch(partitionKey, comp, wideRowMutator);
	}

	@Test
	public void should_batch_remove_property() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).accessors().invoker(invoker).build();

		Composite start = new Composite(), end = new Composite();
		when(compositeFactory.createBaseForQuery(propertyMeta, ComponentEquality.EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(propertyMeta, ComponentEquality.GREATER_THAN_EQUAL)).thenReturn(end);

		persisterImpl.removePropertyBatch(context, propertyMeta);

		verify(entityDao).removeColumnRangeBatch(entity.getId(), start, end, entityMutator);
	}
}
