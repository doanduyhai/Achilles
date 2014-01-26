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
package info.archinnov.achilles.internal.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.statement.cache.StatementCacheKey;
import info.archinnov.achilles.internal.statement.prepared.PreparedStatementGenerator;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class DaoContextFactoryTest {
	@InjectMocks
	private DaoContextFactory builder;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private Session session;

	@Mock
	private PreparedStatementGenerator queryGenerator;

	@Mock
	private PreparedStatement insertPS;

	@Mock
	private PreparedStatement selectForExistenceCheckPS;

	@Mock
	private PreparedStatement selectEagerPS;

	@Mock
	private Map<String, PreparedStatement> removePSs;

	@Mock
	private Map<CQLQueryType, PreparedStatement> counterQueryMap;

	@Mock
	private Map<CQLQueryType, Map<String, PreparedStatement>> clusteredCounterQueryMap;

	@Before
	public void setUp() {
		Whitebox.setInternalState(builder, PreparedStatementGenerator.class, queryGenerator);
	}

	@Test
	public void should_get_insert_ps_transformer() throws Exception {
		// When
		when(queryGenerator.prepareInsertPS(session, entityMeta)).thenReturn(insertPS);

		Function<EntityMeta, PreparedStatement> function = builder.getInsertPSTransformer(session);
		ImmutableList<PreparedStatement> result = FluentIterable.from(Arrays.asList(entityMeta)).transform(function)
				.toImmutableList();

		// Then
		assertThat(result).containsOnly(insertPS);
	}

	@Test
	public void should_get_select_eager_ps_transformer() throws Exception {
		// When
		when(queryGenerator.prepareSelectPS(session, entityMeta)).thenReturn(selectEagerPS);

		Function<EntityMeta, PreparedStatement> function = builder.getSelectPSTransformer(session);
		ImmutableList<PreparedStatement> result = FluentIterable.from(Arrays.asList(entityMeta)).transform(function)
				.toImmutableList();

		// Then
		assertThat(result).containsOnly(selectEagerPS);
	}

	@Test
	public void should_get_remove_ps_transformer() throws Exception {

		// When
		when(queryGenerator.prepareRemovePSs(session, entityMeta)).thenReturn(removePSs);

		Function<EntityMeta, Map<String, PreparedStatement>> function = builder.getRemovePSTransformer(session);
		ImmutableList<Map<String, PreparedStatement>> result = FluentIterable.from(Arrays.asList(entityMeta))
				.transform(function).toImmutableList();

		// Then
		assertThat(result.get(0)).isSameAs(removePSs);
	}

	@Test
	public void should_get_clustered_counter_ps_transformer() throws Exception {

		// When
		when(queryGenerator.prepareClusteredCounterQueryMap(session, entityMeta)).thenReturn(clusteredCounterQueryMap);

		Function<EntityMeta, Map<CQLQueryType, Map<String, PreparedStatement>>> function = builder
				.getClusteredCounterTransformer(session);

		ImmutableList<Map<CQLQueryType, Map<String, PreparedStatement>>> result = FluentIterable
				.from(Arrays.asList(entityMeta)).transform(function).toImmutableList();

		// Then
		assertThat(result.get(0)).isSameAs(clusteredCounterQueryMap);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_dao_context_with_counter() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		EntityMeta meta = new EntityMeta();
		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		meta.setPropertyMetas(ImmutableMap.of("name", nameMeta));
		entityMetaMap.put(CompleteBean.class, meta);

		// When
		when(queryGenerator.prepareInsertPS(session, meta)).thenReturn(insertPS);
		when(queryGenerator.prepareSelectPS(session, meta)).thenReturn(selectEagerPS);
		when(queryGenerator.prepareRemovePSs(session, meta)).thenReturn(removePSs);
		when(queryGenerator.prepareSimpleCounterQueryMap(session)).thenReturn(counterQueryMap);

		DaoContext actual = builder.build(session, entityMetaMap, true);

		// Then
		assertThat((Map<Class<?>, PreparedStatement>) Whitebox.getInternalState(actual, "insertPSs")).containsValue(
				insertPS);
		assertThat((Map<Class<?>, PreparedStatement>) Whitebox.getInternalState(actual, "selectPSs")).containsValue(
				selectEagerPS);
		assertThat((Map<Class<?>, Map<String, PreparedStatement>>) Whitebox.getInternalState(actual, "removePSs"))
				.containsKey(CompleteBean.class);

		assertThat((Cache<StatementCacheKey, PreparedStatement>) Whitebox.getInternalState(actual, "dynamicPSCache"))
				.isInstanceOf(Cache.class);

		assertThat((Map<CQLQueryType, PreparedStatement>) Whitebox.getInternalState(actual, "counterQueryMap"))
				.isSameAs(counterQueryMap);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_dao_context_without_counter() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();
		EntityMeta meta = new EntityMeta();
		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).build();

		meta.setPropertyMetas(ImmutableMap.of("name", nameMeta));
		entityMetaMap.put(CompleteBean.class, meta);

		// When
		when(queryGenerator.prepareInsertPS(session, meta)).thenReturn(insertPS);
		when(queryGenerator.prepareSelectPS(session, meta)).thenReturn(selectEagerPS);
		when(queryGenerator.prepareRemovePSs(session, meta)).thenReturn(removePSs);

		DaoContext actual = builder.build(session, entityMetaMap, false);

		// Then
		assertThat((Map<Class<?>, PreparedStatement>) Whitebox.getInternalState(actual, "insertPSs")).containsValue(
				insertPS);
		assertThat((Map<Class<?>, PreparedStatement>) Whitebox.getInternalState(actual, "selectPSs")).containsValue(
				selectEagerPS);
		assertThat((Map<Class<?>, Map<String, PreparedStatement>>) Whitebox.getInternalState(actual, "removePSs"))
				.containsKey(CompleteBean.class);

		assertThat((Cache<StatementCacheKey, PreparedStatement>) Whitebox.getInternalState(actual, "dynamicPSCache"))
				.isInstanceOf(Cache.class);

		assertThat((Map<CQLQueryType, PreparedStatement>) Whitebox.getInternalState(actual, "counterQueryMap"))
				.isEmpty();
	}
}
