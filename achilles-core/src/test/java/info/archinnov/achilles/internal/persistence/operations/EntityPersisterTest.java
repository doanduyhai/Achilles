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
package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.COUNTER;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.impl.PersisterImpl;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest {
	@InjectMocks
	private EntityPersister persister;

	@Mock
	private PersisterImpl persisterImpl;

	@Mock
	private Session session;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PersistenceContext context;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private EntityMeta entityMeta;

	private Long primaryKey = RandomUtils.nextLong();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

	private List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();

	@Captor
	private ArgumentCaptor<Set<PropertyMeta>> metaSetCaptor;

	@Before
	public void setUp() throws Exception {
		allMetas.clear();
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(context.getEntity()).thenReturn(entity);
		when(context.<CompleteBean> getEntityClass()).thenReturn(CompleteBean.class);
		when(entityMeta.getAllMetasExceptIdMeta()).thenReturn(allMetas);
	}

	@Test
	public void should_persist() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(false);

		PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("count")
				.type(COUNTER).build();

		allMetas.add(counterMeta);

		persister.persist(context);

		verify(persisterImpl).persist(context);
		verify(persisterImpl).persistCounters(eq(context), metaSetCaptor.capture());

		assertThat(metaSetCaptor.getAllValues().get(0)).containsOnly(counterMeta);
	}

	@Test
	public void should_persist_clustered_counter() throws Exception {
		when(entityMeta.isClusteredCounter()).thenReturn(true);

		persister.persist(context);

		verify(persisterImpl).persistClusteredCounter(context);
	}

	@Test
	public void should_remove() throws Exception {
		persister.remove(context);

		verify(persisterImpl).remove(context);
	}
}
