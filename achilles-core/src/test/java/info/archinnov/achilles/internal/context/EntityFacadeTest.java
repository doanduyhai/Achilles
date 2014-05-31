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

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
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

    private Long primaryKey = RandomUtils.nextLong();

    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

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
        Row row = mock(Row.class);
        when(daoContext.loadEntity(context.daoFacade)).thenReturn(row);

        assertThat(facade.loadEntity()).isSameAs(row);
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
        EntityMeta meta = new EntityMeta();
        List<PropertyMeta> pms = new ArrayList<>();
        meta.setAllMetasExceptIdAndCounters(pms);
        context.entityMeta = meta;
        when(configurationContext.getInsertStrategy()).thenReturn(ALL_FIELDS);

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
    public void should_bind_for_removal() throws Exception {
        facade.bindForRemoval("table");

        verify(daoContext).bindForRemoval(context.daoFacade, "table");
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

        Row row = mock(Row.class);
        when(daoContext.getSimpleCounter(context.daoFacade, counterMeta, LOCAL_QUORUM)).thenReturn(row);
        when(row.getLong(CQL_COUNTER_VALUE)).thenReturn(11L);

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
    public void should_bind_for_simple_counter_removal() throws Exception {
        PropertyMeta counterMeta = new PropertyMeta();

        facade.bindForSimpleCounterRemoval(counterMeta);

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
    public void should_bind_for_clustered_counter_removal() throws Exception {
        facade.bindForClusteredCounterRemoval();

        verify(daoContext).bindForClusteredCounterDelete(context.daoFacade);
    }

}
