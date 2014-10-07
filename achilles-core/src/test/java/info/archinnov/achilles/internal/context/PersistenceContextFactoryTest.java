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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceContextFactoryTest {

    private PersistenceContextFactory pmf;

    @Mock
    private DaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private ImmediateFlushContext flushContext;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Before
    public void setUp() throws Exception {
        entityMetaMap = new HashMap<>();
        entityMetaMap.put(CompleteBean.class, meta);

        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        pmf = new PersistenceContextFactory(daoContext, configContext, entityMetaMap);
        Whitebox.setInternalState(pmf, ReflectionInvoker.class, invoker);
    }

    @Test
    public void should_create_new_context_for_entity_with_consistency_and_ttl() throws Exception {
        Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        CompleteBean entity = new CompleteBean(primaryKey);

        when(proxifier.<CompleteBean>deriveBaseClass(entity)).thenReturn(CompleteBean.class);
        when(meta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);

        PersistenceContext actual = pmf.newContext(entity, OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(95));

        assertThat(actual.stateHolderFacade.getEntity()).isSameAs(entity);
        assertThat(actual.stateHolderFacade.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.stateHolderFacade.<CompleteBean>getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(actual.stateHolderFacade.getEntityMeta()).isSameAs(meta);
        assertThat(actual.stateHolderFacade.getIdMeta()).isSameAs(idMeta);
        assertThat(actual.stateHolderFacade.getTtl().get()).isEqualTo(95);
    }

    @Test
    public void should_create_new_context_for_entity() throws Exception {
        Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        CompleteBean entity = new CompleteBean(primaryKey);

        when(proxifier.<CompleteBean>deriveBaseClass(entity)).thenReturn(CompleteBean.class);
        when(meta.forOperations().getPrimaryKey(entity)).thenReturn(primaryKey);

        PersistenceContext actual = pmf.newContext(entity);

        assertThat(actual.stateHolderFacade.getEntity()).isSameAs(entity);
        assertThat(actual.stateHolderFacade.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.stateHolderFacade.<CompleteBean>getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(actual.stateHolderFacade.getEntityMeta()).isSameAs(meta);
        assertThat(actual.stateHolderFacade.getIdMeta()).isSameAs(idMeta);
        assertThat(actual.stateHolderFacade.getTtl()).isSameAs(PersistenceContextFactory.NO_TTL);
    }

    @Test
    public void should_create_new_context_with_primary_key() throws Exception {
        Object primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        PersistenceContext context = pmf.newContext(CompleteBean.class, primaryKey,
                OptionsBuilder.withConsistency(LOCAL_QUORUM).withTtl(98));

        assertThat(context.stateHolderFacade.getEntity()).isNull();
        assertThat(context.stateHolderFacade.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(context.stateHolderFacade.<CompleteBean>getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(context.stateHolderFacade.getEntityMeta()).isSameAs(meta);
        assertThat(context.stateHolderFacade.getIdMeta()).isSameAs(idMeta);
        assertThat(context.stateHolderFacade.getTtl().get()).isEqualTo(98);
    }

    @Test
    public void should_create_new_context_for_slice_query() throws Exception {
        Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        List<Object> partitionComponents = Arrays.<Object>asList(primaryKey);
        when(idMeta.forSliceQueryContext().instantiateEmbeddedIdWithPartitionComponents(partitionComponents)).thenReturn(primaryKey);

        PersistenceContext actual = pmf.newContextForSliceQuery(CompleteBean.class, partitionComponents, EACH_QUORUM);

        assertThat(actual.stateHolderFacade.getEntity()).isNull();
        assertThat(actual.stateHolderFacade.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(actual.stateHolderFacade.<CompleteBean>getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(actual.stateHolderFacade.getEntityMeta()).isSameAs(meta);
        assertThat(actual.stateHolderFacade.getIdMeta()).isSameAs(idMeta);
        assertThat(actual.stateHolderFacade.getTtl().isPresent()).isFalse();
    }
}
