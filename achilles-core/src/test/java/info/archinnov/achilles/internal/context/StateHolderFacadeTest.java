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

import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_ONE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import info.archinnov.achilles.listener.LWTResultListener;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class StateHolderFacadeTest {


    private PersistenceContext context;

    private PersistenceContext.StateHolderFacade facade;

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

    private Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

    private CompleteBean entity = CompleteBeanTestBuilder.builder().id(primaryKey).buid();

    @Before
    public void setUp() throws Exception {
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(configurationContext.getDefaultWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);

        context = new PersistenceContext(meta, configurationContext, daoContext, flushContext, CompleteBean.class, primaryKey, OptionsBuilder.noOptions());
        facade = context.stateHolderFacade;


    }

    @Test
    public void should_get_state() throws Exception {
        //Given
        PropertyMeta nameMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).propertyName("name").accessors().build();
        PropertyMeta counterMeta = PropertyMetaTestBuilder.completeBean(Void.class, Counter.class).propertyName("count").accessors().build();

        when(meta.structure().isClusteredCounter()).thenReturn(true);
        when(meta.getAllCounterMetas()).thenReturn(asList(counterMeta));
        when(meta.getAllMetasExceptCounters()).thenReturn(asList(nameMeta));

        Options.LWTCondition LWTCondition = new Options.LWTCondition("test", "test");
        LWTResultListener listener = mock(LWTResultListener.class);
        final Options options = OptionsBuilder
                .withConsistency(LOCAL_ONE)
                .withTimestamp(100L)
                .withTtl(9)
                .ifEqualCondition("test","test")
                .LWTResultListener(listener);
        context.options = options;
        context.entity = entity;

        //When

        //Then
        assertThat(facade.getEntity()).isSameAs(entity);
        assertThat(facade.<CompleteBean>getEntityClass()).isSameAs(CompleteBean.class);
        assertThat(facade.getEntityMeta()).isSameAs(meta);
        assertThat(facade.getIdMeta()).isSameAs(idMeta);
        assertThat(facade.getPrimaryKey()).isSameAs(primaryKey);
        assertThat(facade.isClusteredCounter()).isTrue();

        assertThat(facade.getOptions()).isSameAs(options);
        assertThat(facade.getTtl().get()).isEqualTo(9);
        assertThat(facade.getTimestamp().get()).isEqualTo(100L);
        assertThat(facade.getConsistencyLevel().get()).isEqualTo(LOCAL_ONE);
        assertThat(facade.hasLWTConditions()).isTrue();
        assertThat(facade.getLWTConditions()).contains(LWTCondition);
        assertThat(facade.getCASResultListener().get()).isSameAs(listener);

        assertThat(facade.getAllCountersMeta()).containsExactly(counterMeta);
        assertThat(facade.getAllGettersExceptCounters()).containsExactly(nameMeta.getGetter());
    }


}
