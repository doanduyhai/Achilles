/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftAbstractFlushContext;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.CompoundTranscoder;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.proxy.wrapper.ListWrapper;
import info.archinnov.achilles.proxy.wrapper.MapWrapper;
import info.archinnov.achilles.proxy.wrapper.SetWrapper;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWrapper;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityInterceptorTest {

	@Mock
	private EntityMeta entityMeta;

	private ThriftEntityInterceptor<CompleteBean> interceptor;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private Map<Method, PropertyMeta> getterMetas;

	@Mock
	private Map<Method, PropertyMeta> setterMetas;

	@Mock
	private Map<Method, PropertyMeta> dirtyMap;

	@Mock
	private Set<Method> alreadyLoaded;

	private List<Method> eagerGetters = new ArrayList<Method>();

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private MethodProxy proxy;

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private Mutator<Long> mutator;

	private ThriftPersistenceContext context;

	@Mock
	private ThriftCounterDao counterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Mock
	private Map<String, ThriftGenericEntityDao> entityDaosMap;

	@Mock
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap;

	@Mock
	private ThriftImmediateFlushContext flushContext;

	@Mock
	private ReflectionInvoker invoker;

	private Long key = 452L;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
			.buid();

	private PropertyMeta idMeta;

	private PropertyMeta nameMeta;

	@Before
	public void setUp() throws Exception {
		idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class)
				.field("id").type(ID).accessors().invoker(invoker).build();

		nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).accessors().build();

		entityMeta = new EntityMeta();
		entityMeta.setIdMeta(idMeta);
		entityMeta.setGetterMetas(getterMetas);
		entityMeta.setSetterMetas(setterMetas);
		entityMeta.setClusteredEntity(false);
		entityMeta.setEagerGetters(eagerGetters);

		when(flushContext.getConsistencyLevel()).thenReturn(ONE);

		context = ThriftPersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class,
						entity.getId()).entity(entity).entityDao(entityDao)
				.entityDaosMap(entityDaosMap)
				.wideRowDaosMap(columnFamilyDaosMap).build();
		context.setFlushContext(flushContext);

		interceptor = ThriftEntityInterceptorBuilder.builder(context, entity)
				.build();

		interceptor.setPrimaryKey(key);
		interceptor.setDirtyMap(dirtyMap);
		interceptor.setContext(context);
		when(flushContext.duplicate()).thenReturn(flushContext);

		Whitebox.setInternalState(interceptor, "alreadyLoaded", alreadyLoaded);
		Whitebox.setInternalState(context, ThriftAbstractFlushContext.class,
				flushContext);
		Whitebox.setInternalState(interceptor, "loader", loader);
	}

	@Test
	public void should_get_id_value_directly() throws Throwable {
		Object key = this.interceptor.intercept(entity, idMeta.getGetter(),
				(Object[]) null, proxy);
		assertThat(key).isEqualTo(key);
	}

	@Test(expected = IllegalAccessException.class)
	public void should_exception_when_setter_called_on_id() throws Throwable {
		this.interceptor.intercept(entity, idMeta.getSetter(),
				new Object[] { 1L }, proxy);
	}

	@Test
	public void should_get_unmapped_property() throws Throwable {
		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");
		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);

		assertThat(name).isEqualTo("name");

		verify(getterMetas).containsKey(nameMeta.getGetter());
		verify(setterMetas).containsKey(nameMeta.getGetter());
	}

	@Test
	public void should_load_lazy_property() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SIMPLE);
		when(alreadyLoaded.contains(nameMeta.getGetter())).thenReturn(false);
		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);

		assertThat(name).isEqualTo("name");

		verify(loader).loadPropertyIntoObject(context, entity, propertyMeta);
		verify(alreadyLoaded).add(nameMeta.getGetter());
	}

	@Test
	public void should_return_already_loaded_lazy_property() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SIMPLE);

		when(alreadyLoaded.contains(nameMeta.getGetter())).thenReturn(true);

		when(proxy.invoke(entity, (Object[]) null)).thenReturn("name");

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);

		assertThat(name).isEqualTo("name");

		verifyZeroInteractions(loader);
		verify(alreadyLoaded, never()).add(nameMeta.getGetter());
	}

	@Test
	public void should_set_property() throws Throwable {
		when(setterMetas.containsKey(nameMeta.getSetter())).thenReturn(true);
		when(setterMetas.get(nameMeta.getSetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.SIMPLE);

		Object[] args = new Object[] { "sdfsdvdqfv" };

		when(proxy.invoke(entity, args)).thenReturn(null);
		Object name = this.interceptor.intercept(entity, nameMeta.getSetter(),
				args, proxy);

		assertThat(name).isNull();

		verify(proxy).invoke(entity, args);
		verify(dirtyMap).put(nameMeta.getSetter(), propertyMeta);
	}

	@Test
	public void should_create_list_wrapper() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);

		when(proxy.invoke(entity, null)).thenReturn(Arrays.asList("a"));

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);

		assertThat(name).isInstanceOf(ListWrapper.class);
	}

	@Test
	public void should_return_null_when_no_list() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);

		when(proxy.invoke(entity, null)).thenReturn(null);

		Object actual = this.interceptor.intercept(entity,
				nameMeta.getGetter(), (Object[]) null, proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_create_set_wrapper() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		when(proxy.invoke(entity, null)).thenReturn(new HashSet<String>());

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);

		assertThat(name).isInstanceOf(SetWrapper.class);
	}

	@Test
	public void should_return_null_when_no_set() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.SET);

		when(proxy.invoke(entity, null)).thenReturn(null);

		Object actual = this.interceptor.intercept(entity,
				nameMeta.getGetter(), (Object[]) null, proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_create_map_wrapper() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(LAZY_MAP);

		when(proxy.invoke(entity, null)).thenReturn(
				new HashMap<Integer, String>());

		Object name = this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);

		assertThat(name).isInstanceOf(MapWrapper.class);
	}

	@Test
	public void should_return_null_when_no_map() throws Throwable {
		when(getterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(getterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(LAZY_MAP);

		when(proxy.invoke(entity, null)).thenReturn(null);

		Object actual = this.interceptor.intercept(entity,
				nameMeta.getGetter(), (Object[]) null, proxy);

		assertThat(actual).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_counter_wrapper_for_entity() throws Throwable {
		CompleteBean bean = new CompleteBean();
		String propertyName = "count";

		Method countGetter = CompleteBean.class.getDeclaredMethod("getCount");

		when(getterMetas.containsKey(countGetter)).thenReturn(true);
		when(getterMetas.get(countGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(COUNTER);
		CounterProperties counterProperties = new CounterProperties("fqcn");
		counterProperties.setIdMeta(idMeta);

		when(propertyMeta.getCounterProperties()).thenReturn(counterProperties);
		when(propertyMeta.getPropertyName()).thenReturn(propertyName);
		Object counterWrapper = this.interceptor.intercept(bean, countGetter,
				(Object[]) null, proxy);

		assertThat(counterWrapper).isInstanceOf(ThriftCounterWrapper.class);

		Composite comp = (Composite) Whitebox.getInternalState(counterWrapper,
				"key");
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("fqcn");
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("452");

		Composite columnName = (Composite) Whitebox.getInternalState(
				counterWrapper, "columnName");
		assertThat(columnName.getComponent(0).getValue(STRING_SRZ)).isEqualTo(
				propertyName);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_counter_wrapper_for_clustered_entity()
			throws Throwable {
		CompleteBean bean = new CompleteBean();
		Long userId = RandomUtils.nextLong();
		String name = "name";
		CompoundKey compoundKey = new CompoundKey(userId, name);

		Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");
		PropertyMeta clusteredIdMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).type(EMBEDDED_ID)
				.compGetters(Arrays.asList(userIdGetter, nameGetter))
				.compClasses(Long.class, String.class)
				.transcoder(new CompoundTranscoder(new ObjectMapper()))
				.invoker(invoker).build();

		entityMeta.setClusteredEntity(true);
		Method countGetter = CompleteBean.class.getDeclaredMethod("getCount");
		when(getterMetas.containsKey(countGetter)).thenReturn(true);
		when(getterMetas.get(countGetter)).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(COUNTER);
		CounterProperties counterProperties = new CounterProperties("fqcn");
		counterProperties.setIdMeta(clusteredIdMeta);

		when(invoker.getPartitionKey(compoundKey, clusteredIdMeta)).thenReturn(
				userId);
		when(propertyMeta.getCounterProperties()).thenReturn(counterProperties);

		interceptor.setPrimaryKey(compoundKey);

		Object counterWrapper = this.interceptor.intercept(bean, countGetter,
				(Object[]) null, proxy);

		assertThat(counterWrapper).isInstanceOf(ThriftCounterWrapper.class);

		assertThat(Whitebox.getInternalState(counterWrapper, "key")).isSameAs(
				userId);
		Composite columnName = (Composite) Whitebox.getInternalState(
				counterWrapper, "columnName");
		assertThat(columnName.getComponent(0).getValue(STRING_SRZ)).isEqualTo(
				name);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_call_setter_on_counter() throws Throwable {
		when(setterMetas.containsKey(nameMeta.getGetter())).thenReturn(true);
		when(setterMetas.get(nameMeta.getGetter())).thenReturn(propertyMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.COUNTER);

		this.interceptor.intercept(entity, nameMeta.getGetter(),
				(Object[]) null, proxy);
	}

}
