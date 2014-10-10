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

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.internal.proxy.dirtycheck.SimpleDirtyChecker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityUpdaterTest {

    @InjectMocks
    private EntityUpdater entityUpdater;

    @Mock
    private CounterPersister counterPersister;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private ProxyInterceptor<CompleteBean> interceptor;

    @Mock
    private PersistenceContext.EntityFacade context;

    @Mock
    private PropertyMeta pm;

    @Mock
    private EntityMeta meta;

    @Captor
    private ArgumentCaptor<List<PropertyMeta>> pmCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private List<PropertyMeta> allMetas = new ArrayList<>();

    private List<PropertyMeta> allCounterMetas = new ArrayList<>();

    private Map<Method, DirtyChecker> dirtyMap = new HashMap<>();

    @Before
    public void setUp() {

        when(context.getEntity()).thenReturn(entity);
        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntityMeta()).thenReturn(meta);

        allMetas.clear();
        dirtyMap.clear();
    }

    @Test
    public void should_update_proxified_entity() throws Exception {
        when(proxifier.isProxy(entity)).thenReturn(true);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
        when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
        when(meta.getAllCounterMetas()).thenReturn(allCounterMetas);

        PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).propertyName("user").type(SIMPLE)
                .accessors().build();
        DirtyChecker dirtyChecker = new SimpleDirtyChecker(pm);
        dirtyMap.put(pm.getGetter(), dirtyChecker);
        when(context.isClusteredCounter()).thenReturn(false);

        entityUpdater.update(context, entity);

        verify(context).setEntity(entity);
        verify(context).pushUpdateStatement(pmCaptor.capture());

        assertThat(pmCaptor.getValue()).containsOnly(pm);

        verify(counterPersister).persistCounters(context, allCounterMetas);
        verify(interceptor).setEntityOperations(context);
        verify(interceptor).setTarget(entity);

    }

    @Test
    public void should_update_proxified_clustered_counter_entity() throws Exception {
        when(proxifier.isProxy(entity)).thenReturn(true);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
        when(interceptor.getDirtyMap()).thenReturn(dirtyMap);

        PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).propertyName("user").type(SIMPLE)
                .accessors().build();
        DirtyChecker dirtyChecker = new SimpleDirtyChecker(pm);
        dirtyMap.put(pm.getGetter(), dirtyChecker);
        when(context.isClusteredCounter()).thenReturn(true);

        entityUpdater.update(context, entity);

        verify(context).setEntity(entity);
        verify(context).pushUpdateStatement(pmCaptor.capture());

        assertThat(pmCaptor.getValue()).containsOnly(pm);

        verify(counterPersister).persistClusteredCounters(context);
        verify(interceptor).setEntityOperations(context);
        verify(interceptor).setTarget(entity);

    }
}
