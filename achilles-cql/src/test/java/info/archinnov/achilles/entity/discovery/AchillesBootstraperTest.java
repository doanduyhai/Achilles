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

package info.archinnov.achilles.entity.discovery;

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.DaoContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.SchemaContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.test.more.entity.Entity3;
import info.archinnov.achilles.test.parser.entity.UserBean;
import info.archinnov.achilles.test.sample.entity.Entity1;
import info.archinnov.achilles.test.sample.entity.Entity2;
import info.archinnov.achilles.type.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class AchillesBootstraperTest {

	private AchillesBootstraper bootstraper = new AchillesBootstraper();

	@Mock
	private EntityParser parser;

	@Mock
	private DaoContextFactory factory;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private SchemaContext schemaContext;

	@Mock
	private EntityMeta meta;

	@Mock
	private TableMetadata tableMeta;

	@Mock
	private Session session;

	@Captor
	private ArgumentCaptor<EntityParsingContext> contextCaptor;

	@Before
	public void setUp() {

		Whitebox.setInternalState(bootstraper, EntityParser.class, parser);
		Whitebox.setInternalState(bootstraper, DaoContextFactory.class, factory);
	}

	@Test
	public void should_find_entities_from_multiple_packages() throws Exception {
		List<Class<?>> entities = bootstraper.discoverEntities(Arrays.asList(
				"info.archinnov.achilles.test.sample.entity", "info.archinnov.achilles.test.more.entity"));

		assertThat(entities).hasSize(3);
		assertThat(entities).contains(Entity1.class);
		assertThat(entities).contains(Entity2.class);
		assertThat(entities).contains(Entity3.class);
	}

	@Test
	public void should_find_entity_from_one_package() throws Exception {
		List<Class<?>> entities = bootstraper.discoverEntities(Arrays
				.asList("info.archinnov.achilles.test.more.entity"));
		assertThat(entities).hasSize(1);
		assertThat(entities).contains(Entity3.class);
	}

	@Test
	public void should_build_meta_data() throws Exception {
		// Given
		List<Class<?>> entities = Arrays.<Class<?>> asList(UserBean.class);

		// When
		when(parser.parseEntity(contextCaptor.capture())).thenReturn(meta);

		Pair<Map<Class<?>, EntityMeta>, Boolean> pair = bootstraper.buildMetaDatas(configContext, entities);

		assertThat(pair.left.get(UserBean.class)).isSameAs(meta);
		assertThat(pair.right).isFalse();
	}

	@Test
	public void should_validate_tables() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> metas = ImmutableMap.<Class<?>, EntityMeta> of(UserBean.class, meta);
		Map<String, TableMetadata> tableMetaDatas = ImmutableMap.<String, TableMetadata> of("userbean", tableMeta);

		// When
		when(schemaContext.fetchTableMetaData()).thenReturn(tableMetaDatas);
		when(meta.getTableName()).thenReturn("UserBean");
		when(schemaContext.entityMetaEntrySet()).thenReturn(metas.entrySet());
		when(schemaContext.hasSimpleCounter()).thenReturn(false);

		bootstraper.validateOrCreateTables(schemaContext);

		// Then
		verify(schemaContext).validateForEntity(meta, tableMeta);
	}

	@Test
	public void should_create_tables() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> metas = ImmutableMap.<Class<?>, EntityMeta> of(UserBean.class, meta);
		Map<String, TableMetadata> tableMetaDatas = ImmutableMap.<String, TableMetadata> of();

		// When
		when(schemaContext.fetchTableMetaData()).thenReturn(tableMetaDatas);
		when(meta.getTableName()).thenReturn("UserBean");
		when(schemaContext.entityMetaEntrySet()).thenReturn(metas.entrySet());
		when(schemaContext.hasSimpleCounter()).thenReturn(false);

		bootstraper.validateOrCreateTables(schemaContext);

		// Then
		verify(schemaContext).createTableForEntity(meta);
	}

	@Test
	public void should_validate_counter_table() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> metas = ImmutableMap.<Class<?>, EntityMeta> of(UserBean.class, meta);
		Map<String, TableMetadata> tableMetaDatas = ImmutableMap.<String, TableMetadata> of(CQL_COUNTER_TABLE,
				tableMeta);

		// When
		when(schemaContext.fetchTableMetaData()).thenReturn(tableMetaDatas);
		when(meta.getTableName()).thenReturn("UserBean");
		when(schemaContext.entityMetaEntrySet()).thenReturn(metas.entrySet());
		when(schemaContext.hasSimpleCounter()).thenReturn(true);

		bootstraper.validateOrCreateTables(schemaContext);

		// Then
		verify(schemaContext).validateAchillesCounter();
	}

	@Test
	public void should_create_counter_table() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> metas = ImmutableMap.<Class<?>, EntityMeta> of(UserBean.class, meta);
		Map<String, TableMetadata> tableMetaDatas = ImmutableMap.<String, TableMetadata> of();

		// When
		when(schemaContext.fetchTableMetaData()).thenReturn(tableMetaDatas);
		when(meta.getTableName()).thenReturn("UserBean");
		when(schemaContext.entityMetaEntrySet()).thenReturn(metas.entrySet());
		when(schemaContext.hasSimpleCounter()).thenReturn(true);

		bootstraper.validateOrCreateTables(schemaContext);

		// Then
		verify(schemaContext).createTableForCounter();
	}

	@Test
	public void should_build_dao_context() throws Exception {
		// Given
		Map<Class<?>, EntityMeta> entityMetaMap = ImmutableMap.<Class<?>, EntityMeta> of();
		DaoContext daoContext = mock(DaoContext.class);

		// When
		when(factory.build(session, entityMetaMap, true)).thenReturn(daoContext);

		DaoContext actual = bootstraper.buildDaoContext(session, entityMetaMap, true);

		// Then
		assertThat(actual).isSameAs(daoContext);

	}
}
