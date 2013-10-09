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

import static info.archinnov.achilles.type.BoundingMode.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.CQLSliceQueryIterator;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Row;

@RunWith(MockitoJUnitRunner.class)
public class CQLSliceQueryExecutorTest {

	private CQLSliceQueryExecutor executor;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private ConfigurationContext configContext;

	@Mock
	private CQLStatementGenerator generator;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLEntityMapper mapper;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private CQLDaoContext daoContext;

	@Mock
	private CQLPersistenceContextFactory contextFactory;

	@Mock
	private CQLEntityProxifier proxifier;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private Iterator<Row> iterator;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PropertyMeta idMeta;

	private SliceQuery<ClusteredEntity> sliceQuery;

	private ClusteredEntity entity;

	private EntityMeta meta;

	private Long partitionKey = RandomUtils.nextLong();

	private List<Object> partitionComponents = Arrays.<Object> asList(partitionKey);
	private List<Object> clusteringsFrom = Arrays.<Object> asList("name1");
	private List<Object> clusteringsTo = Arrays.<Object> asList("name2");
	private int limit = 98;
	private int batchSize = 99;

	@Before
	public void setUp() {
		when(configContext.getConsistencyPolicy().getDefaultGlobalReadConsistencyLevel()).thenReturn(EACH_QUORUM);

		executor = new CQLSliceQueryExecutor(contextFactory, configContext, daoContext);
		executor.proxifier = proxifier;
		Whitebox.setInternalState(executor, CQLStatementGenerator.class, generator);
		Whitebox.setInternalState(executor, CQLEntityMapper.class, mapper);

		meta = new EntityMeta();
		meta.setEagerGetters(new ArrayList<Method>());
		meta.setIdMeta(idMeta);
		Whitebox.setInternalState(meta, ReflectionInvoker.class, invoker);

		when(idMeta.getComponentNames()).thenReturn(Arrays.asList("id", "name"));
		when(idMeta.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, String.class));

		sliceQuery = new SliceQuery<ClusteredEntity>(ClusteredEntity.class, meta, partitionComponents, clusteringsFrom,
				clusteringsTo, ASCENDING, EXCLUSIVE_BOUNDS, LOCAL_QUORUM, limit, batchSize, true,false, null);

	}

	@Test
	public void should_get_clustered_entities() throws Exception {

		Query query = mock(Query.class);
		when(generator.generateSelectSliceQuery(anySliceQuery(), eq(limit))).thenReturn(query);

		Row row = mock(Row.class);
		List<Row> rows = Arrays.asList(row);
		when(daoContext.execute(query).all()).thenReturn(rows);

		when(invoker.instanciate(ClusteredEntity.class)).thenReturn(entity);
		when(contextFactory.newContext(entity)).thenReturn(context);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		assertThat(executor.get(sliceQuery)).containsOnly(entity);
		verify(mapper).setEagerPropertiesToEntity(row, meta, entity);
	}

	@Test
	public void should_create_iterator_for_clustered_entities() throws Exception {
		Query query = mock(Query.class);
		when(generator.generateSelectSliceQuery(anySliceQuery(), eq(limit))).thenReturn(query);
		when(daoContext.execute(query).iterator()).thenReturn(iterator);

		PreparedStatement ps = mock(PreparedStatement.class);
		when(generator.generateIteratorSliceQuery(anySliceQuery(), eq(daoContext))).thenReturn(ps);
		when(contextFactory.newContextForSliceQuery(ClusteredEntity.class, partitionComponents, LOCAL_QUORUM))
				.thenReturn(context);

		Iterator<ClusteredEntity> iter = executor.iterator(sliceQuery);

		assertThat(iter).isNotNull();
		assertThat(iter).isInstanceOf(CQLSliceQueryIterator.class);
	}

	@Test
	public void should_remove_clustered_entities() throws Exception {
		sliceQuery = new SliceQuery<ClusteredEntity>(ClusteredEntity.class, meta, partitionComponents,
				Arrays.<Object> asList(), Arrays.<Object> asList(), ASCENDING, EXCLUSIVE_BOUNDS, LOCAL_QUORUM, limit,
				batchSize, false,false, null);

		Query query = mock(Query.class);
		when(generator.generateRemoveSliceQuery(anySliceQuery())).thenReturn(query);

		executor.remove(sliceQuery);

		verify(daoContext).execute(query);

	}

	private CQLSliceQuery<ClusteredEntity> anySliceQuery() {
		return Mockito.<CQLSliceQuery<ClusteredEntity>> any();
	}
}
