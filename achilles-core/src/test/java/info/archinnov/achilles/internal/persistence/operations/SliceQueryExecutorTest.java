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

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.iterator.SliceQueryIterator;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.type.ConsistencyLevel;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryExecutorTest {

    @InjectMocks
    private SliceQueryExecutor executor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurationContext configContext;

    @Mock
    private BoundStatementWrapper bsWrapper;

    @Mock
    private EntityMapper mapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DaoContext daoContext;

    @Mock
    private PersistenceContextFactory contextFactory;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private PersistenceContext context;

    @Mock
    private PersistenceContext.EntityFacade entityFacade;

    @Mock
    private Iterator<Row> iterator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    private SliceQueryProperties<ClusteredEntity> sliceQueryProperties;

    @Mock
    private ClusteredEntity entity;

    private Long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

    private List<Object> partitionComponents = Arrays.<Object>asList(partitionKey);

    private ConsistencyLevel defaultReadLevel = ConsistencyLevel.EACH_QUORUM;
    private ConsistencyLevel defaultWriteLevel = ConsistencyLevel.LOCAL_QUORUM;

    @Before
    public void setUp() {
        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(defaultReadLevel);
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(defaultWriteLevel);
        when(context.getEntityFacade()).thenReturn(entityFacade);
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.forSliceQuery().getClusteringOrderForSliceQuery()).thenReturn(new ClusteringOrder("col", Sorting.ASC));

        sliceQueryProperties = SliceQueryProperties.builder(meta,ClusteredEntity.class, SliceQueryProperties.SliceType.SELECT);

        executor = new SliceQueryExecutor(contextFactory,configContext,daoContext);

        Whitebox.setInternalState(executor, EntityProxifier.class, proxifier);
        Whitebox.setInternalState(executor, EntityMapper.class, mapper);
    }

    @Test
    public void should_get_clustered_entities() throws Exception {

        Row row = mock(Row.class);
        List<Row> rows = asList(row);

        when(daoContext.bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel)).thenReturn(bsWrapper);
        when(daoContext.execute(bsWrapper).all()).thenReturn(rows);

        when(meta.forOperations().instanciate()).thenReturn(entity);
        when(contextFactory.newContext(entity)).thenReturn(context);
        when(proxifier.buildProxyWithAllFieldsLoadedExceptCounters(entity, entityFacade)).thenReturn(entity);

        List<ClusteredEntity> actual = executor.get(sliceQueryProperties);

        verify(daoContext).bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel);

        assertThat(actual).containsOnly(entity);
        verify(meta.forInterception()).intercept(entity, Event.POST_LOAD);
        verify(mapper).setNonCounterPropertiesToEntity(row, meta, entity);
    }

    @Test
    public void should_create_iterator_for_clustered_entities() throws Exception {
        when(daoContext.bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel)).thenReturn(bsWrapper);
        when(daoContext.execute(bsWrapper).iterator()).thenReturn(iterator);

        when(contextFactory.newContextForSliceQuery(ClusteredEntity.class, partitionComponents, LOCAL_QUORUM))
                .thenReturn(context);

        Iterator<ClusteredEntity> iter = executor.iterator(sliceQueryProperties);

        assertThat(iter).isNotNull();
        assertThat(iter).isInstanceOf(SliceQueryIterator.class);
    }

    @Test
    public void should_remove_clustered_entities() throws Exception {
        when(daoContext.bindForSliceQueryDelete(sliceQueryProperties, defaultWriteLevel)).thenReturn(bsWrapper);

        executor.delete(sliceQueryProperties);

        verify(daoContext).execute(bsWrapper);

    }
}
