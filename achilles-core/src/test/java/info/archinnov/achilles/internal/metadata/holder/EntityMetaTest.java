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
package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.interceptor.Event.POST_INSERT;
import static info.archinnov.achilles.interceptor.Event.PRE_INSERT;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COMPOUND_PRIMARY_KEY;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.fest.assertions.api.Assertions;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.internal.utils.Pair;

public class EntityMetaTest {
    @Test
    public void should_to_string() throws Exception {
        Map<String, PropertyMeta> propertyMetas = new HashMap<>();
        propertyMetas.put("name", null);
        propertyMetas.put("age", null);

        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.SIMPLE).consistencyLevels(Pair.create(ALL, ALL)).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClassName("className");
        entityMeta.setQualifiedTableName("cfName");
        entityMeta.setIdClass(Long.class);
        entityMeta.setPropertyMetas(propertyMetas);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setClusteredEntity(true);
        entityMeta.setConsistencyLevels(Pair.create(ONE, ONE));

        StringBuilder toString = new StringBuilder();
        toString.append("EntityMeta{className=className, ");
        toString.append("qualifiedTableName=cfName, ");
        toString.append("propertyMetas=age,name, ");
        toString.append("idMeta=").append(idMeta.toString()).append(", ");
        toString.append("clusteredEntity=true, ");
        toString.append("consistencyLevels=(ONE,ONE)}");
        assertThat(entityMeta.toString()).isEqualTo(toString.toString());
    }

    @Test
    public void should_get_all_metas() throws Exception {

        PropertyMeta pm1 = new PropertyMeta();
        PropertyMeta pm2 = new PropertyMeta();

        Map<String, PropertyMeta> propertyMetas = new HashMap<>();
        propertyMetas.put("name", pm1);
        propertyMetas.put("age", pm2);

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setPropertyMetas(propertyMetas);

        assertThat(entityMeta.getAllMetas()).containsExactly(pm1, pm2);
    }

    @Test
    public void should_return_false_for_is_clustered_counter_if_not_clustered() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta counterMeta = PropertyMetaTestBuilder
                //
                .completeBean(Void.class, Long.class).propertyName("count").type(COUNTER).build();

        entityMeta.setClusteredEntity(false);
        entityMeta.setPropertyMetas(ImmutableMap.of("count", counterMeta));

        assertThat(entityMeta.structure().isClusteredCounter()).isFalse();
    }

    @Test
    public void should_return_false_for_is_clustered_counter_if_more_than_one_property() throws Exception {
        EntityMeta entityMeta = new EntityMeta();

        PropertyMeta nameMeta = PropertyMetaTestBuilder
                //
                .completeBean(Void.class, String.class).propertyName("name").type(SIMPLE).build();

        PropertyMeta counterMeta = PropertyMetaTestBuilder
                //
                .completeBean(Void.class, Long.class).propertyName("count").type(COUNTER).build();

        entityMeta.setClusteredEntity(true);
        entityMeta.setPropertyMetas(ImmutableMap.of("name", nameMeta, "count", counterMeta));

        assertThat(entityMeta.structure().isClusteredCounter()).isFalse();
    }

    @Test
    public void should_return_false_for_is_clustered_counter_if_value_less() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.PARTITION_KEY).build();

        entityMeta.setClusteredEntity(false);
        entityMeta.setPropertyMetas(ImmutableMap.of("idMeta", idMeta));

        assertThat(entityMeta.structure().isClusteredCounter()).isFalse();
    }

    @Test
    public void should_return_false_for_is_clustered_counter_if_not_counter_type() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.PARTITION_KEY).build();

        PropertyMeta nameMeta = PropertyMetaTestBuilder
                //
                .completeBean(Void.class, String.class).propertyName("name").type(SIMPLE).build();
        entityMeta.setClusteredEntity(true);
        entityMeta.setPropertyMetas(ImmutableMap.of("idMeta", idMeta, "nameMeta", nameMeta));

        assertThat(entityMeta.structure().isClusteredCounter()).isFalse();
    }

    @Test
    public void should_return_true_when_value_less() throws Exception {
        EntityMeta entityMeta = new EntityMeta();

        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.PARTITION_KEY).build();

        entityMeta.setPropertyMetas(ImmutableMap.of("idMeta", idMeta));

        assertThat(entityMeta.structure().isValueless()).isTrue();
    }

    @Test
    public void should_return_true_when_has_compound_pk() throws Exception {
        PropertyMeta idMeta = new PropertyMeta();
        idMeta.setType(COMPOUND_PRIMARY_KEY);
        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);

        assertThat(meta.structure().isCompoundPK()).isTrue();
    }

    @Test
    public void should_return_all_property_metas_for_columnsMetaToLoad() throws Exception {
        //Given
        final PropertyMeta propertyMeta = new PropertyMeta();
        propertyMeta.setEntityClassName("CompleteBean");
        propertyMeta.setPropertyName("property");
        propertyMeta.setType(PropertyType.SIMPLE);

        EntityMeta meta = new EntityMeta();
        meta.setPropertyMetas(ImmutableMap.of("property", propertyMeta));

        //When
        meta.setClusteredCounter(true);

        //Then
        assertThat(meta.forOperations().getColumnsMetaToLoad()).containsExactly(propertyMeta);
    }

    @Test
    public void should_return_allMetasExceptCounters_for_columnsMetaToLoad() throws Exception {
        //Given
        final ArrayList<PropertyMeta> allMetasExceptCounters = new ArrayList<>();
        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptCounters(allMetasExceptCounters);

        //When
        meta.setClusteredCounter(false);

        //Then
        assertThat(meta.forOperations().getColumnsMetaToLoad()).isEqualTo(allMetasExceptCounters);
    }



    @Test
    public void should_return_event_interceptors_for_specific_event() throws Exception {
        // Given
        EntityMeta entityMeta = new EntityMeta();
        Interceptor<String> postPersistInterceptor = createInterceptor(POST_INSERT);
        Interceptor<String> prePersistInterceptor = createInterceptor(PRE_INSERT);

        // When
        entityMeta.forInterception().addInterceptor(postPersistInterceptor);
        entityMeta.forInterception().addInterceptor(prePersistInterceptor);

        // Then
        assertThat(entityMeta.forInterception().getInterceptorsForEvent(POST_INSERT)).containsExactly(postPersistInterceptor);
        assertThat(entityMeta.forInterception().getInterceptorsForEvent(PRE_INSERT)).containsExactly(prePersistInterceptor);
    }

    @Test
    public void should_apply_right_interceptor_on_right_event() throws Exception {

        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).propertyName("id")
                .type(PropertyType.COMPOUND_PRIMARY_KEY).accessors().build();
//        idMeta.setInvoker(new ReflectionInvoker());
        entityMeta.setIdMeta(idMeta);
        entityMeta.forInterception().addInterceptor(createInterceptorForCompleteBean(PRE_INSERT, 30L));
        entityMeta.forInterception().addInterceptor(createInterceptorForCompleteBean(POST_INSERT, 35L));

        entityMeta.forInterception().intercept(bean, PRE_INSERT);
        Assertions.assertThat(bean.getAge()).isEqualTo(30L);
        entityMeta.forInterception().intercept(bean, POST_INSERT);
        Assertions.assertThat(bean.getAge()).isEqualTo(35L);
    }

    @Test
    public void should_retrieve_all_properties_meta_for_insert() throws Exception {
        //Given
        EntityMeta entityMeta = new EntityMeta();
        List<PropertyMeta> pms = new ArrayList<>();
        entityMeta.setAllMetasExceptIdAndCounters(pms);
        entityMeta.setInsertStrategy(ALL_FIELDS);

        //When
        final List<PropertyMeta> propertyMetas = entityMeta.forOperations().retrievePropertyMetasForInsert(new CompleteBean());

        //Then
        assertThat(propertyMetas).isSameAs(pms);
    }

    private Interceptor<String> createInterceptor(final Event event) {
        Interceptor<String> interceptor = new Interceptor<String>() {

            @Override
            public void onEvent(String entity) {
            }

            @Override
            public List<Event> events() {
                List<Event> events = new ArrayList<>();
                events.add(event);
                return events;
            }
        };
        return interceptor;
    }

    private Interceptor<CompleteBean> createInterceptorForCompleteBean(final Event event, final long age) {
        Interceptor<CompleteBean> interceptor = new Interceptor<CompleteBean>() {

            @Override
            public void onEvent(CompleteBean entity) {
                entity.setAge(age);
            }

            @Override
            public List<Event> events() {
                return asList(event);
            }
        };
        return interceptor;
    }

    @Test
    public void should_test() throws Exception {
        //Given
        Pattern pattern = Pattern.compile("writing request [A-Z]+ .+\\[cl=");

        String text = "TRACE [21:31:47,052][] com.datastax.driver.core.Connection@:write [localhost/127.0.0.1:9184-4] writing request EXECUTE 0xd138318ced105ea0cd75474c40cbe55c ([cl=ONE, vals=[java.nio.HeapByteBuffer[pos=0 lim=8 cap=8], java.nio.HeapByteBuffer[pos=0 lim=11 cap=12], java.nio.HeapByteBuffer[pos=0 lim=4 cap=4], java.nio.HeapByteBuffer[pos=0 lim=4 cap=4]], skip=false, psize=5000, state=null, serialCl=LOCAL_SERIAL])";

        assertThat(pattern.matcher(text).find()).isTrue();
        //When

        //Then

    }
}
