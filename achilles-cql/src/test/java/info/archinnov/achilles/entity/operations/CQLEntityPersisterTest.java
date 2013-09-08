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
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLPersisterImpl;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityPersisterTest {
	@InjectMocks
	private CQLEntityPersister persister;

	@Mock
	private CQLPersisterImpl persisterImpl;

	@Mock
	private Session session;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private CQLPersistenceContext context;

	@Mock
	private CQLPersistenceContext joinContext;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private EntityMeta joinMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder()
			.id(primaryKey).buid();

	private List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();

	@Captor
	private ArgumentCaptor<Set<PropertyMeta>> metaSetCaptor;

	@Captor
	private ArgumentCaptor<Insert> insertCaptor;

	@Captor
	private ArgumentCaptor<Batch> batchCaptor;

	@Captor
	private ArgumentCaptor<List<Statement>> statementsCaptor;

	@Before
	public void setUp() throws Exception {
		allMetas.clear();
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(context.getEntity()).thenReturn(entity);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(
				CompleteBean.class);
		when(entityMeta.getAllMetasExceptIdMeta()).thenReturn(allMetas);
	}

	@Test
	public void should_persist() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(false);
		when(context.addToProcessingList(entity)).thenReturn(true);

		PropertyMeta joinMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(JOIN_SIMPLE).cascadeType(CascadeType.ALL).build();

		PropertyMeta counterMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("count")
				.type(COUNTER).build();

		allMetas.add(joinMeta);
		allMetas.add(counterMeta);

		persister.persist(context);

		verify(persisterImpl).persist(context);
		verify(persisterImpl).cascadePersist(eq(persister), eq(context),
				metaSetCaptor.capture());
		verify(persisterImpl).persistCounters(eq(context),
				metaSetCaptor.capture());

		assertThat(metaSetCaptor.getAllValues().get(0)).containsOnly(joinMeta);
		assertThat(metaSetCaptor.getAllValues().get(1)).containsOnly(
				counterMeta);
	}

	@Test
	public void should_persist_clustered_counter() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(context.addToProcessingList(entity)).thenReturn(true);

		persister.persist(context);

		verify(persisterImpl).persistClusteredCounter(context);

	}

	@Test
	public void should_not_persist_twice_the_same_entity() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(false);
		when(context.addToProcessingList(entity)).thenReturn(true, false);
		persister.persist(context);
		persister.persist(context);

		verify(persisterImpl, times(1)).persist(context);
		verify(persisterImpl, times(1)).cascadePersist(eq(persister),
				eq(context), metaSetCaptor.capture());

		assertThat(metaSetCaptor.getValue()).isEmpty();
	}

	@Test
	public void should_ensure_entity_exist() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(false);
		when(context.isEnsureJoinConsistency()).thenReturn(true);
		when(context.addToProcessingList(entity)).thenReturn(true);

		PropertyMeta joinMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(JOIN_SIMPLE).build();

		allMetas.add(joinMeta);

		persister.persist(context);
		verify(persisterImpl).persist(context);
		verify(persisterImpl).ensureEntitiesExist(eq(context),
				metaSetCaptor.capture());

		assertThat(metaSetCaptor.getValue()).containsOnly(joinMeta);
	}

	@Test
	public void should_remove() throws Exception {
		persister.remove(context);

		verify(persisterImpl).remove(context);
	}
}
