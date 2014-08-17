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
package info.archinnov.achilles.internal.proxy;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class ProxyInterceptorBuilderTest {

    @Mock
    private PersistenceContext.EntityFacade context;

    private CompleteBean entity = new CompleteBean();

    @Test
    public void should_build_interceptor_with_eager_fields_already_loaded() throws Exception {

        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("id").build();

        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.getClassName()).thenReturn("classname");
        when(meta.getGetterMetas()).thenReturn(new HashMap<Method, PropertyMeta>());
        when(meta.getSetterMetas()).thenReturn(new HashMap<Method, PropertyMeta>());

        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getPrimaryKey()).thenReturn(entity.getId());

        ProxyInterceptor<CompleteBean> interceptor = ProxyInterceptorBuilder.builder(context, entity)
                .alreadyLoaded(Sets.newHashSet(idMeta.getGetter())).build();

        assertThat(interceptor.getEntityOperations()).isSameAs(context);
        assertThat(interceptor.getTarget()).isSameAs(entity);
        assertThat(interceptor.getPrimaryKey()).isEqualTo(entity.getId());
        assertThat(interceptor.getAlreadyLoaded()).containsOnly(idMeta.getGetter());
    }

    @Test
    public void should_build_interceptor_with_no_eager_fields() throws Exception {

        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("id").build();

        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.getClassName()).thenReturn("classname");
        when(meta.getGetterMetas()).thenReturn(new HashMap<Method, PropertyMeta>());
        when(meta.getSetterMetas()).thenReturn(new HashMap<Method, PropertyMeta>());

        when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getPrimaryKey()).thenReturn(entity.getId());

        ProxyInterceptor<CompleteBean> interceptor = ProxyInterceptorBuilder.<CompleteBean>builder(context, entity)
                .build();

        assertThat(interceptor.getEntityOperations()).isSameAs(context);
        assertThat(interceptor.getTarget()).isSameAs(entity);
        assertThat(interceptor.getPrimaryKey()).isEqualTo(entity.getId());
        assertThat(interceptor.getAlreadyLoaded()).isEmpty();
    }
}
