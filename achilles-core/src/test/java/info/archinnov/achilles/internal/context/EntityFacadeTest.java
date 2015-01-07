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

import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class EntityFacadeTest {

    private PersistenceContext context;

    private PersistenceContext.EntityFacade facade;

    @Mock
    private DaoContext daoContext;

    @Mock
    private AbstractFlushContext flushContext;

    @Mock
    private ConfigurationContext configurationContext;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;


    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock
    private DirtyCheckChangeSet changeSet;

    @Mock
    private ListenableFuture<Row> futureRow;

    private Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

    @Before
    public void setUp() throws Exception {
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(configurationContext.getDefaultWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);

        context = new PersistenceContext(meta, configurationContext, daoContext, flushContext, CompleteBean.class, primaryKey, OptionsBuilder.noOptions());
        facade = context.entityFacade;
    }

    @Test
    public void should_eager_load_entity() throws Exception {
        when(daoContext.loadEntity(context.daoFacade)).thenReturn(futureRow);

        assertThat(facade.loadEntity()).isSameAs(futureRow);
    }

    @Test
    public void should_load_property() throws Exception {
        Row row = mock(Row.class);
        when(daoContext.loadProperty(context.daoFacade, idMeta)).thenReturn(row);

        assertThat(facade.loadProperty(idMeta)).isSameAs(row);
    }

    @Test
    public void should_push_insert() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        List<PropertyMeta> pms = new ArrayList<>();
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(pms);
        context.entityMeta = meta;
        when(configurationContext.getGlobalInsertStrategy()).thenReturn(ALL_FIELDS);

        //When
        facade.pushInsertStatement();

        //Then
        verify(daoContext).pushInsertStatement(context.daoFacade, pms);
    }

    @Test
    public void should_push_update() throws Exception {
        List<PropertyMeta> pms = Arrays.asList();
        facade.pushUpdateStatement(pms);

        verify(daoContext).pushUpdateStatement(context.daoFacade, pms);
    }

    @Test
    public void should_push_for_collection_and_map_update() throws Exception {
        facade.pushCollectionAndMapUpdateStatements(changeSet);

        verify(daoContext).pushCollectionAndMapUpdateStatement(context.daoFacade, changeSet);
    }

    @Test
    public void should_bind_for_deletion() throws Exception {
        facade.bindForDeletion();

        verify(daoContext).bindForDeletion(context.daoFacade, meta);
    }


    // Simple counter
    @Test
    public void should_bind_for_simple_counter_increment() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        facade.bindForSimpleCounterIncrement(counterMeta, 11L);

        verify(daoContext).bindForSimpleCounterIncrement(context.daoFacade, counterMeta, 11L);
    }

    @Test
    public void should_get_simple_counter() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        when(daoContext.getSimpleCounter(context.daoFacade, counterMeta, LOCAL_QUORUM)).thenReturn(11L);

        Long counterValue = facade.getSimpleCounter(counterMeta, LOCAL_QUORUM);

        assertThat(counterValue).isEqualTo(11L);
    }

    @Test
    public void should_return_null_when_no_simple_counter_value() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        when(daoContext.getSimpleCounter(context.daoFacade, counterMeta, LOCAL_QUORUM)).thenReturn(null);

        assertThat(facade.getSimpleCounter(counterMeta, LOCAL_QUORUM)).isNull();
    }

    @Test
    public void should_bind_for_simple_counter_deletion() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        facade.bindForSimpleCounterDeletion(counterMeta);

        verify(daoContext).bindForSimpleCounterDelete(context.daoFacade, counterMeta);
    }

    // Clustered counter
    @Test
    public void should_push_clustered_counter_increment() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        facade.pushClusteredCounterIncrementStatement(counterMeta, 11L);

        verify(daoContext).pushClusteredCounterIncrementStatement(context.daoFacade, counterMeta, 11L);
    }


    @Test
    public void should_get_clustered_counter() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();
        counterMeta.setPropertyName("count");
        Long counterValue = 11L;

        when(daoContext.getClusteredCounterColumn(context.daoFacade, counterMeta)).thenReturn(counterValue);

        Long actual = facade.getClusteredCounterColumn(counterMeta);

        assertThat(actual).isEqualTo(counterValue);
    }

    @Test
    public void should_return_null_when_no_clustered_counter_value() throws Exception {

        when(daoContext.getClusteredCounter(context.daoFacade)).thenReturn(null);

        assertThat(facade.getClusteredCounter()).isNull();
    }

    @Test
    public void should_bind_for_clustered_counter_deletion() throws Exception {
        facade.bindForClusteredCounterDeletion();

        verify(daoContext).bindForClusteredCounterDelete(context.daoFacade);
    }

}
