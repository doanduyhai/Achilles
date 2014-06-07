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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityRefresherTest {

    @InjectMocks
    private EntityRefresher refresher;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private EntityLoader loader;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private ListenableFuture<CompleteBean> futureEntity;

    @Mock
    private AchillesFuture<CompleteBean> achillesFutureEntity;

    @Captor
    private ArgumentCaptor<Function<CompleteBean, CompleteBean>> interceptorCaptor;

    @Mock
    private ProxyInterceptor<CompleteBean> proxyInterceptor;

    @Mock
    private Map<Method, DirtyChecker> dirtyMap;

    @Mock
    private Set<Method> alreadyLoaded;

    @Mock
    private Set<Method> allGettersExceptCounters;

    @Mock
    private PersistenceContext.EntityFacade context;

    @Before
    public void setUp() {
        when(context.getExecutorService()).thenReturn(executorService);
    }

    @Test
    public void should_refresh() throws Exception {
        // Given
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getPrimaryKey()).thenReturn(bean.getId());
        when(context.getEntity()).thenReturn(bean);

        when(proxifier.getInterceptor(bean)).thenReturn(proxyInterceptor);

        when(proxyInterceptor.getTarget()).thenReturn(bean);
        when(proxyInterceptor.getDirtyMap()).thenReturn(dirtyMap);
        when(proxyInterceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);
        when(context.getEntityMeta()).thenReturn(entityMeta);
        when(loader.load(context, CompleteBean.class)).thenReturn(achillesFutureEntity);
        when(context.getAllGettersExceptCounters()).thenReturn(allGettersExceptCounters);
        when(asyncUtils.transformFuture(eq(achillesFutureEntity), interceptorCaptor.capture(), eq(executorService))).thenReturn(achillesFutureEntity);
        when(asyncUtils.buildInterruptible(achillesFutureEntity)).thenReturn(achillesFutureEntity);

        // When
        final AchillesFuture<CompleteBean> actual = refresher.refresh(bean, context);

        // Then
        assertThat(actual).isSameAs(achillesFutureEntity);

        final CompleteBean actualBean = interceptorCaptor.getValue().apply(bean);
        assertThat(actualBean).isSameAs(bean);

        verify(dirtyMap).clear();
        verify(alreadyLoaded).clear();
        verify(alreadyLoaded).addAll(allGettersExceptCounters);
        verify(proxyInterceptor).setTarget(bean);
    }

    @Test(expected = AchillesStaleObjectStateException.class)
    public void should_throw_exception_when_object_staled() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        refresher.updateProxyInterceptor(context, proxyInterceptor, bean, bean.getId()).apply(null);
    }
}
