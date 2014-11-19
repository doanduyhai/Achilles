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

package info.archinnov.achilles.internal.persistence.operations;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntityWithCounter;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.Arrays;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityValidatorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private EntityValidator entityValidator;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PropertyMeta idMeta;

	@Mock
	private PersistenceContext context;

	@Before
	public void setUp() {
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
	}

	@Test
	public void should_validate() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(proxifier.<CompleteBean> deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(proxifier.getRealObject(bean)).thenReturn(bean);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(entityMeta.forOperations().getPrimaryKey(bean)).thenReturn(12L);

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@Test
	public void should_exception_when_no_id() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(proxifier.<CompleteBean> deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(proxifier.getRealObject(bean)).thenReturn(bean);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(entityMeta.forOperations().getPrimaryKey(bean)).thenReturn(null);

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot get primary key for entity " + CompleteBean.class.getCanonicalName());

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@Test
	public void should_validate_clustered_id() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		EmbeddedKey clusteredId = new EmbeddedKey(11L, "name");

		when(entityMeta.forOperations().getPrimaryKey(bean)).thenReturn(clusteredId);
        when(proxifier.getRealObject(bean)).thenReturn(bean);
		when(idMeta.structure().isEmbeddedId()).thenReturn(true);
		when(idMeta.forTranscoding().encodeToComponents(clusteredId,true)).thenReturn(Arrays.<Object> asList(11L, "name"));

		entityValidator.validateEntity(bean, entityMeta);
	}

    @Test
    public void should_validate_clustered_id_for_static_columns_only() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        EmbeddedKey clusteredId = new EmbeddedKey(11L, null);
        PropertyMeta staticColumnMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(entityMeta.forOperations().getPrimaryKey(bean)).thenReturn(clusteredId);
        when(entityMeta.hasStaticColumns()).thenReturn(true);
        when(entityMeta.getAllMetasExceptIdAndCounters()).thenReturn(Arrays.asList(staticColumnMeta));
        when(staticColumnMeta.structure().isStaticColumn()).thenReturn(true);
        when(staticColumnMeta.forValues().getValueFromField(clusteredId)).thenReturn("static");

        when(proxifier.getRealObject(bean)).thenReturn(bean);
        when(idMeta.structure().isEmbeddedId()).thenReturn(true);
        when(idMeta.forTranscoding().encodeToComponents(clusteredId,true)).thenReturn(Arrays.<Object> asList(11L, "static"));

        entityValidator.validateEntity(bean, entityMeta);
    }

	@Test
	public void should_validate_simple_id() throws Exception {
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when(proxifier.getRealObject(bean)).thenReturn(bean);
		when(entityMeta.forOperations().getPrimaryKey(bean)).thenReturn(12L);
		when(idMeta.structure().isEmbeddedId()).thenReturn(false);

		entityValidator.validateEntity(bean, entityMeta);
	}

	@Test
	public void should_validate_not_clustered_counter() throws Exception {
		ClusteredEntityWithCounter entity = new ClusteredEntityWithCounter();
		when(proxifier.<ClusteredEntityWithCounter> deriveBaseClass(entity)).thenReturn(
				ClusteredEntityWithCounter.class);
		when(entityMetaMap.get(ClusteredEntityWithCounter.class)).thenReturn(entityMeta);
		when(entityMeta.structure().isClusteredCounter()).thenReturn(false);
		entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
	}

	@Test
	public void should_exception_when_not_clustered_counter() throws Exception {
		ClusteredEntityWithCounter entity = new ClusteredEntityWithCounter();
		when(proxifier.<ClusteredEntityWithCounter> deriveBaseClass(entity)).thenReturn(
				ClusteredEntityWithCounter.class);
		when(entityMetaMap.get(ClusteredEntityWithCounter.class)).thenReturn(entityMeta);
		when(entityMeta.structure().isClusteredCounter()).thenReturn(true);

		exception.expect(AchillesException.class);
		exception.expectMessage("The entity '" + entity
				+ "' is a clustered counter and does not support insert/update with TTL");

		entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
	}

}
