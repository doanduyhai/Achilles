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

package info.archinnov.achilles.internal.persistence.discovery;

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.DaoContextFactory;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.SchemaContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.parsing.EntityParser;
import info.archinnov.achilles.internal.persistence.parsing.context.EntityParsingContext;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
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
public class AchillesBootstrapperTest {

	private AchillesBootstrapper bootstrapper = new AchillesBootstrapper();

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

		Whitebox.setInternalState(bootstrapper, EntityParser.class, parser);
		Whitebox.setInternalState(bootstrapper, DaoContextFactory.class, factory);
	}

	@Test
	public void should_find_entities_from_multiple_packages() throws Exception {
		List<Class<?>> entities = bootstrapper.discoverEntities(Arrays.asList(
				"info.archinnov.achilles.test.sample.entity", "info.archinnov.achilles.test.more.entity"));

		assertThat(entities).hasSize(3);
		assertThat(entities).contains(Entity1.class);
		assertThat(entities).contains(Entity2.class);
		assertThat(entities).contains(Entity3.class);
	}

	@Test
	public void should_find_entity_from_one_package() throws Exception {
		List<Class<?>> entities = bootstrapper.discoverEntities(Arrays
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

		Pair<Map<Class<?>, EntityMeta>, Boolean> pair = bootstrapper.buildMetaDatas(configContext, entities);

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

		bootstrapper.validateOrCreateTables(schemaContext);

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

		bootstrapper.validateOrCreateTables(schemaContext);

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

		bootstrapper.validateOrCreateTables(schemaContext);

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

		bootstrapper.validateOrCreateTables(schemaContext);

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

		DaoContext actual = bootstrapper.buildDaoContext(session, entityMetaMap, true);

		// Then
		assertThat(actual).isSameAs(daoContext);
	}

    @Test
    public void should_add_event_interceptors_to_entity_metas() throws Exception {
        //Given
        final EntityMeta metaString = new EntityMeta();
        final EntityMeta metaLong = new EntityMeta();
        final List<Interceptor<?>> interceptors = Arrays.<Interceptor<?>>asList(stringInterceptor1,stringInterceptor2,longInterceptor);
        final Map<Class<?>, EntityMeta> entityMetaMap = ImmutableMap.<Class<?>, EntityMeta>of(String.class, metaString,Long.class,metaLong);

        //When
        bootstrapper.addInterceptorsToEntityMetas(interceptors, entityMetaMap);

        //Then
        assertThat(metaString.getInterceptors()).contains(stringInterceptor1,stringInterceptor2);
        assertThat(metaLong.getInterceptors()).contains(longInterceptor);
    }

    private Interceptor<String> stringInterceptor1 = new Interceptor<String>() {
        @Override
        public void onEvent(String entity) {
        }

        @Override
        public List<Event> events() {
            return null;
        }
    };

    private Interceptor<String> stringInterceptor2 = new Interceptor<String>() {
        @Override
        public void onEvent(String entity) {
        }

        @Override
        public List<Event> events() {
            return null;
        }
    };

    private Interceptor<Long> longInterceptor = new Interceptor<Long>() {
        @Override
        public void onEvent(Long entity) {
        }

        @Override
        public List<Event> events() {
            return null;
        }
    };
}
