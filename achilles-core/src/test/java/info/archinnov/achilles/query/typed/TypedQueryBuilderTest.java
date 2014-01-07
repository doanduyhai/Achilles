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
package info.archinnov.achilles.query.typed;

import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.persistence.operations.EntityMapper;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyType;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.Row;

@RunWith(MockitoJUnitRunner.class)
public class TypedQueryBuilderTest {

	private TypedQueryBuilder<CompleteBean> builder;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private DaoContext daoContext;

	@Mock
	private EntityMapper mapper;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private PersistenceContextFactory contextFactory;

	@Mock
	private PersistenceContext context;

	@Mock
	private Row row;

	@Mock
	private EntityMeta meta;

	private Class<CompleteBean> entityClass = CompleteBean.class;

	private CompleteBean entity = new CompleteBean();

	@Test
	public void should_get_all_managed_with_select_star() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID)
				.accessors().build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).accessors().build();

		EntityMeta meta = buildEntityMeta(idMeta, nameMeta);

		String queryString = "select * from test";
		initBuilder(queryString, meta, meta.getPropertyMetas(), true);

		when(daoContext.execute(any(AbstractStatementWrapper.class)).all()).thenReturn(Arrays.asList(row));
		when(mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>> any(),
						eq(true))).thenReturn(entity);
		when(contextFactory.newContext(entity)).thenReturn(context);
		when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context)).thenReturn(entity);

		List<CompleteBean> actual = builder.get();

		assertThat(actual).containsExactly(entity);

        verify(meta).intercept(entity, Event.POST_LOAD);
	}

	@Test
	public void should_get_all_managed_with_normal_select() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).accessors().build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).accessors().build();

		PropertyMeta ageMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("age")
				.type(PropertyType.SIMPLE).accessors().build();

		EntityMeta meta = buildEntityMeta(idMeta, nameMeta, ageMeta);

		String queryString = " select id, name   from  test";
		initBuilder(queryString, meta, meta.getPropertyMetas(), true);

		when(daoContext.execute(any(AbstractStatementWrapper.class)).all()).thenReturn(Arrays.asList(row));
		when(
				mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>> any(),
						eq(true))).thenReturn(entity);
		when(contextFactory.newContext(entity)).thenReturn(context);
		when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context)).thenReturn(entity);

		List<CompleteBean> actual = builder.get();

		assertThat(actual).containsExactly(entity);

        verify(meta).intercept(entity, Event.POST_LOAD);
	}

	@Test
	public void should_get_all_skipping_null_entity() throws Exception {
		EntityMeta meta = buildEntityMeta();
		initBuilder("select * from test", meta, meta.getPropertyMetas(), true);

		when(daoContext.execute(any(AbstractStatementWrapper.class)).all()).thenReturn(Arrays.asList(row));
		when(
				mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>> any(),
						eq(true))).thenReturn(null);

		List<CompleteBean> actual = builder.get();

		assertThat(actual).isEmpty();
        verify(meta,never()).intercept(entity, Event.POST_LOAD);
	}

	@Test
	public void should_get_all_raw_entities() throws Exception {

		EntityMeta meta = mock(EntityMeta.class);
		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();

		String queryString = "select * from test";
		initBuilder(queryString, meta, propertyMetas, false);

		when(daoContext.execute(any(AbstractStatementWrapper.class)).all()).thenReturn(Arrays.asList(row));
		when(mapper.mapRowToEntityWithPrimaryKey(meta, row, propertyMetas, false)).thenReturn(entity);

		List<CompleteBean> actual = builder.get();

		assertThat(actual).containsExactly(entity);
        verify(meta).intercept(entity, Event.POST_LOAD);
		verifyZeroInteractions(contextFactory, proxifier);
	}

	@Test
	public void should_get_first_managed_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).accessors().build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(PropertyType.SIMPLE).accessors().build();

		EntityMeta meta = buildEntityMeta(idMeta, nameMeta);

		String queryString = "select id from test";
		initBuilder(queryString, meta, meta.getPropertyMetas(), true);

		when(daoContext.execute(any(AbstractStatementWrapper.class)).one()).thenReturn(row);
		when(
				mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>> any(),
						eq(true))).thenReturn(entity);
		when(contextFactory.newContext(entity)).thenReturn(context);
		when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, context)).thenReturn(entity);

		CompleteBean actual = builder.getFirst();

		assertThat(actual).isSameAs(entity);
        verify(meta).intercept(entity, Event.POST_LOAD);
	}

	@Test
	public void should_get_first_raw_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(ID)
				.accessors().build();

		PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).accessors().build();

		EntityMeta meta = buildEntityMeta(idMeta, nameMeta);
		String queryString = "select id from test";
		initBuilder(queryString, meta, meta.getPropertyMetas(), false);

		when(daoContext.execute(any(AbstractStatementWrapper.class)).one()).thenReturn(row);
		when(
				mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>> any(),
						eq(false))).thenReturn(entity);

		CompleteBean actual = builder.getFirst();

		assertThat(actual).isSameAs(entity);
        verify(meta).intercept(entity, Event.POST_LOAD);
		verifyZeroInteractions(contextFactory, proxifier);
	}

	@Test
	public void should_return_null_when_null_row() throws Exception {
		EntityMeta meta = buildEntityMeta();
		String queryString = "select id from test";
		initBuilder(queryString, meta, meta.getPropertyMetas(), false);
		when(daoContext.execute(any(AbstractStatementWrapper.class)).one()).thenReturn(null);
		CompleteBean actual = builder.getFirst();

		assertThat(actual).isNull();

		verifyZeroInteractions(contextFactory, proxifier);
        verify(meta,never()).intercept(entity, Event.POST_LOAD);
	}

	@Test
	public void should_return_null_when_cannot_map_entity() throws Exception {
		EntityMeta meta = buildEntityMeta();
		String queryString = "select id from test";
		initBuilder(queryString, meta, meta.getPropertyMetas(), false);
		when(daoContext.execute(any(AbstractStatementWrapper.class)).one()).thenReturn(row);
		when(
				mapper.mapRowToEntityWithPrimaryKey(eq(meta), eq(row), Mockito.<Map<String, PropertyMeta>> any(),
						eq(true))).thenReturn(null);

		CompleteBean actual = builder.getFirst();

		assertThat(actual).isNull();

		verifyZeroInteractions(contextFactory, proxifier);
	}

	private EntityMeta buildEntityMeta(PropertyMeta... pms) {
		EntityMeta meta = mock(EntityMeta.class);
		Map<String, PropertyMeta> propertyMetas = new HashMap();
		for (PropertyMeta pm : pms) {
			propertyMetas.put(pm.getPropertyName(), pm);
		}

        when(meta.getPropertyMetas()).thenReturn(propertyMetas);
		return meta;
	}

	private void initBuilder(String queryString, EntityMeta meta, Map<String, PropertyMeta> propertyMetas,
			boolean managed) {
		builder = new TypedQueryBuilder(entityClass, daoContext, queryString, meta, contextFactory,
				managed, true, new Object[] { "a" });

		Whitebox.setInternalState(builder, String.class, queryString);
		Whitebox.setInternalState(builder, Map.class, propertyMetas);
		Whitebox.setInternalState(builder, EntityMapper.class, mapper);
		Whitebox.setInternalState(builder, PersistenceContextFactory.class, contextFactory);
		Whitebox.setInternalState(builder, EntityProxifier.class, proxifier);
	}
}
