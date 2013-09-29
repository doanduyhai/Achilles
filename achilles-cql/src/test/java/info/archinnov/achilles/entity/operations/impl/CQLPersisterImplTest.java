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
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class CQLPersisterImplTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private CQLPersisterImpl persisterImpl = new CQLPersisterImpl();

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLEntityPersister entityPersister;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	private List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp() {
		when(context.getEntity()).thenReturn(entity);
		when(context.getPrimaryKey()).thenReturn(entity.getId());
		when(context.getEntityMeta()).thenReturn(entityMeta);

		when(entityMeta.getAllMetas()).thenReturn(allMetas);
		when(entityMeta.getAllMetasExceptIdMeta()).thenReturn(allMetas);
		allMetas.clear();
	}

	@Test
	public void should_persist() throws Exception {
		persisterImpl.persist(context);

		verify(context).pushInsertStatement();
	}

	@Test
	public void should_persist_clustered_counter() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.accessors().invoker(invoker).build();
		Counter counter = CounterBuilder.incr();

		when(context.getFirstMeta()).thenReturn(counterMeta);
		when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(counter);

		persisterImpl.persistClusteredCounter(context);

		verify(context).pushClusteredCounterIncrementStatement(counterMeta, 1L);
	}

	@Test
	public void should_exception_when_null_value_for_clustered_counter() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.accessors().invoker(invoker).build();

		when(context.getFirstMeta()).thenReturn(counterMeta);
		when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(null);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("Cannot insert clustered counter entity '" + entity
				+ "' with null clustered counter value");
		persisterImpl.persistClusteredCounter(context);

	}

	@Test
	public void should_persist_counters() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.accessors().invoker(invoker).build();

		when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(CounterBuilder.incr(12L));

		persisterImpl.persistCounters(context, Sets.newHashSet(counterMeta));

		verify(context).bindForSimpleCounterIncrement(counterMeta, 12L);
	}

	@Test
	public void should_not_persist_counters_when_no_counter_set() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.accessors().invoker(invoker).build();

		when(invoker.getValueFromField(entity, counterMeta.getGetter())).thenReturn(null);

		persisterImpl.persistCounters(context, Sets.newHashSet(counterMeta));

		verify(context, never()).bindForSimpleCounterIncrement(eq(counterMeta), any(Long.class));
	}

	@Test
	public void should_remove() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(false);
		when(entityMeta.getTableName()).thenReturn("table");
		when(entityMeta.getWriteConsistencyLevel()).thenReturn(EACH_QUORUM);

		persisterImpl.remove(context);

		verify(context).bindForRemoval("table");
	}

	@Test
	public void should_remove_clustered_counter() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.accessors().build();

		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(entityMeta.getFirstMeta()).thenReturn(counterMeta);

		persisterImpl.remove(context);

		verify(context).bindForClusteredCounterRemoval(counterMeta);
	}

	@Test
	public void should_remove_linked_counters() throws Exception {
		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).field("user")
				.type(COUNTER).invoker(invoker).consistencyLevels(Pair.create(ConsistencyLevel.ONE, EACH_QUORUM))
				.build();

		allMetas.add(counterMeta);

		persisterImpl.removeLinkedCounters(context);

		verify(context).bindForSimpleCounterRemoval(counterMeta);
	}
}
