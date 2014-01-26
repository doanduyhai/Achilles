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
package info.archinnov.achilles.internal.statement.prepared;

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.*;
import static info.archinnov.achilles.test.builders.PropertyMetaTestBuilder.completeBean;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class PreparedStatementBinderTest {
	private final Optional<Integer> ttlO = Optional.fromNullable(null);

	@InjectMocks
	private PreparedStatementBinder binder;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private PreparedStatement ps;

	@Mock
	private BoundStatement bs;

	@Mock
	private DataTranscoder transcoder;

	@Mock
	private ObjectMapper objectMapper;

	private EntityMeta entityMeta;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp() {
		entityMeta = new EntityMeta();
	}

	@Test
	public void should_bind_for_insert_with_simple_id() throws Exception {
		long primaryKey = RandomUtils.nextLong();
		long age = RandomUtils.nextLong();
		String name = "name";

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(SIMPLE).accessors()
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").type(SIMPLE).accessors()
				.transcoder(transcoder).invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdAndCounters(asList(nameMeta, ageMeta));
		entityMeta.setClusteredCounter(false);

		when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
		when(invoker.getValueFromField(entity, nameMeta.getField())).thenReturn(name);
		when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(age);

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
		when(transcoder.encode(nameMeta, name)).thenReturn(name);
		when(transcoder.encode(ageMeta, age)).thenReturn(age);

		when(ps.bind(Matchers.<Object> anyVararg())).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForInsert(ps, entityMeta, entity, ALL, ttlO);

		verify(bs).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(primaryKey, name, age, 0);
	}

	@Test
	public void should_bind_for_insert_with_null_fields() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").type(SIMPLE).accessors()
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").type(SIMPLE).accessors()
				.transcoder(transcoder).invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdAndCounters(asList(nameMeta, ageMeta));
		entityMeta.setClusteredCounter(false);

		long primaryKey = RandomUtils.nextLong();
		String name = "name";
		when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
		when(invoker.getValueFromField(entity, nameMeta.getField())).thenReturn(name);
		when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(null);

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
		when(transcoder.encode(nameMeta, name)).thenReturn(name);
		when(transcoder.encode(eq(ageMeta), any())).thenReturn(null);

		when(ps.bind(Matchers.<Object> anyVararg())).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForInsert(ps, entityMeta, entity, ALL, ttlO);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(primaryKey, name, null, 0);
	}

	@Test
	public void should_bind_for_insert_with_compound_key() throws Exception {
		long userId = RandomUtils.nextLong();
		long age = RandomUtils.nextLong();
		String name = "name";
		List<Object> friends = Arrays.<Object> asList("foo", "bar");
		Set<Object> followers = Sets.<Object> newHashSet("George", "Paul");
		Map<Object, Object> preferences = ImmutableMap.<Object, Object> of(1, "FR");

		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(EMBEDDED_ID)
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").type(SIMPLE).transcoder(transcoder)
				.accessors().invoker(invoker).build();

		PropertyMeta friendsMeta = completeBean(Void.class, String.class).field("friends").type(LIST)
				.transcoder(transcoder).accessors().invoker(invoker).build();

		PropertyMeta followersMeta = completeBean(Void.class, Long.class).field("followers").type(SET)
				.transcoder(transcoder).accessors().invoker(invoker).build();

		PropertyMeta preferencesMeta = completeBean(Void.class, Long.class).field("preferences").type(MAP)
				.transcoder(transcoder).accessors().invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetasExceptIdAndCounters(asList(ageMeta, friendsMeta, followersMeta, preferencesMeta));
		entityMeta.setClusteredCounter(false);

		EmbeddedKey embeddedKey = new EmbeddedKey(userId, name);

		when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(embeddedKey);
		when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(age);
		when(invoker.getValueFromField(entity, friendsMeta.getField())).thenReturn(friends);
		when(invoker.getValueFromField(entity, followersMeta.getField())).thenReturn(followers);
		when(invoker.getValueFromField(entity, preferencesMeta.getField())).thenReturn(preferences);

		when(transcoder.encodeToComponents(idMeta, embeddedKey)).thenReturn(Arrays.<Object> asList(userId, name));
		when(transcoder.encode(ageMeta, age)).thenReturn(age);
		when(transcoder.encode(friendsMeta, friends)).thenReturn(friends);
		when(transcoder.encode(followersMeta, followers)).thenReturn(followers);
		when(transcoder.encode(preferencesMeta, preferences)).thenReturn(preferences);

		when(ps.bind(Matchers.<Object> anyVararg())).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForInsert(ps, entityMeta, entity, ALL, ttlO);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(userId, name, age, friends, followers, preferences, 0);
	}

	@Test
	public void should_bind_with_only_pk_in_where_clause() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
				.transcoder(transcoder).invoker(invoker).build();
		entityMeta.setIdMeta(idMeta);
		long primaryKey = RandomUtils.nextLong();

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);

		when(ps.bind(Matchers.<Object> anyVararg())).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, primaryKey, ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(primaryKey);
	}

	@Test
	public void should_bind_for_update() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().type(ID)
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta nameMeta = completeBean(Void.class, String.class).field("name").accessors().type(SIMPLE)
				.transcoder(transcoder).invoker(invoker).build();

		PropertyMeta ageMeta = completeBean(Void.class, Long.class).field("age").accessors().type(SIMPLE)
				.transcoder(transcoder).invoker(invoker).build();

		entityMeta.setIdMeta(idMeta);

		long primaryKey = RandomUtils.nextLong();
		long age = RandomUtils.nextLong();
		String name = "name";

		when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
		when(invoker.getValueFromField(entity, nameMeta.getField())).thenReturn(name);
		when(invoker.getValueFromField(entity, ageMeta.getField())).thenReturn(age);

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
		when(transcoder.encode(nameMeta, name)).thenReturn(name);
		when(transcoder.encode(ageMeta, age)).thenReturn(age);

		when(ps.bind(Matchers.<Object> anyVararg())).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForUpdate(ps, entityMeta, asList(nameMeta, ageMeta), entity, ALL,
				ttlO);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(0, name, age, primaryKey);
	}

	@Test
	public void should_bind_for_simple_counter_increment_decrement() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).invoker(invoker)
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		PropertyMeta counterMeta = completeBean(Void.class, Long.class).field("count").transcoder(transcoder)
				.invoker(invoker).build();

		Long primaryKey = RandomUtils.nextLong();
		Long counter = RandomUtils.nextLong();

		when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
		when(transcoder.forceEncodeToJSON(counter)).thenReturn(counter.toString());
		when(ps.bind(counter, "CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta, primaryKey,
				counter, ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(counter, "CompleteBean", primaryKey.toString(), "count");
	}

	@Test
	public void should_bind_for_simple_counter_select() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).invoker(invoker)
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		PropertyMeta counterMeta = completeBean(Void.class, Long.class).field("count").transcoder(transcoder)
				.invoker(invoker).build();

		Long primaryKey = RandomUtils.nextLong();

		when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
		when(ps.bind("CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForSimpleCounterSelect(ps, meta, counterMeta, primaryKey, ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly("CompleteBean", primaryKey.toString(), "count");
	}

	@Test
	public void should_bind_for_simple_counter_delete() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).invoker(invoker)
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		PropertyMeta counterMeta = completeBean(Void.class, Long.class).field("count").transcoder(transcoder)
				.invoker(invoker).build();

		Long primaryKey = RandomUtils.nextLong();

		when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
		when(ps.bind("CompleteBean", primaryKey.toString(), "count")).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForSimpleCounterDelete(ps, meta, counterMeta, primaryKey, ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly("CompleteBean", primaryKey.toString(), "count");
	}

	@Test
	public void should_bind_for_clustered_counter_increment_decrement() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID)
				.invoker(invoker).build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		Long primaryKey = RandomUtils.nextLong();
		Long counter = RandomUtils.nextLong();

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
		when(ps.bind(counter, primaryKey)).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForClusteredCounterIncrementDecrement(ps, meta, primaryKey, counter,
				ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(counter, primaryKey);

	}

	@Test
	public void should_bind_for_clustered_counter_select() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID)
				.invoker(invoker).build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		Long primaryKey = RandomUtils.nextLong();

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
		when(ps.bind(primaryKey)).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForClusteredCounterSelect(ps, meta, primaryKey, ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
		assertThat(asList(actual.getValues())).containsExactly(primaryKey);

	}

	@Test
	public void should_bind_for_clustered_counter_delete() throws Exception {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").transcoder(transcoder).type(ID)
				.invoker(invoker).build();

		EntityMeta meta = new EntityMeta();
		meta.setClassName("CompleteBean");
		meta.setIdMeta(idMeta);

		Long primaryKey = RandomUtils.nextLong();

		when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
		when(ps.bind(primaryKey)).thenReturn(bs);

		BoundStatementWrapper actual = binder.bindForClusteredCounterDelete(ps, meta, primaryKey, ALL);

		verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);

		assertThat(asList(actual.getValues())).containsExactly(primaryKey);
	}
}
