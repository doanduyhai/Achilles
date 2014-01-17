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
package info.archinnov.achilles.internal.proxy;

import static info.archinnov.achilles.test.builders.PropertyMetaTestBuilder.completeBean;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.persistence.operations.CounterLoader;
import info.archinnov.achilles.internal.persistence.operations.EntityLoader;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterImpl;
import info.archinnov.achilles.internal.proxy.wrapper.ListWrapper;
import info.archinnov.achilles.internal.proxy.wrapper.MapWrapper;
import info.archinnov.achilles.internal.proxy.wrapper.SetWrapper;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class EntityInterceptorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private EntityInterceptor<CompleteBean> interceptor;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PersistenceContext context;

	@Mock
	private EntityLoader loader;

	@Mock
	private CounterLoader counterLoader;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private MethodProxy proxy;

	@Mock
	private PropertyMeta pm;

	private Object[] args = new Object[] {};

	private Map<Method, PropertyMeta> getterMetas = new HashMap<>();
	private Map<Method, PropertyMeta> setterMetas = new HashMap<>();
	private Set<Method> alreadyLoaded = new HashSet<Method>();
	private Map<Method, PropertyMeta> dirtyMap = new HashMap<>();
	private CompleteBean target;
	private Long key = RandomUtils.nextLong();
	private Object rawValue = "raw";
	private PropertyMeta idMeta;

	@Before
	public void setUp() throws Throwable {

		getterMetas.clear();
		setterMetas.clear();

		interceptor.setGetterMetas(getterMetas);
		interceptor.setSetterMetas(setterMetas);

		target = CompleteBeanTestBuilder.builder().id(key).buid();
		interceptor.setTarget(target);
		interceptor.setPrimaryKey(key);
		interceptor.setContext(context);
		interceptor.setDirtyMap(dirtyMap);
		interceptor.setAlreadyLoaded(alreadyLoaded);

		dirtyMap.clear();
		alreadyLoaded.clear();

		idMeta = completeBean(Void.class, Long.class).field("id").accessors().build();

		interceptor.setIdGetter(idMeta.getGetter());
		interceptor.setIdSetter(idMeta.getSetter());

		Whitebox.setInternalState(interceptor, "loader", loader);
		Whitebox.setInternalState(interceptor, "counterLoader", counterLoader);
		Whitebox.setInternalState(interceptor, "invoker", invoker);
	}

	@Test
	public void should_return_key_when_invoking_id_getter() throws Throwable {
		PropertyMeta idMeta = completeBean(Void.class, Long.class).field("id").accessors().build();

		interceptor.setIdGetter(idMeta.getGetter());

		Object id = interceptor.intercept(target, idMeta.getGetter(), args, null);

		assertThat(id).isEqualTo(key);
	}

	@Test
	public void should_exception_when_setting_id() throws Throwable {
		exception.expect(IllegalAccessException.class);
		exception.expectMessage("Cannot change primary key value for existing entity ");

		interceptor.intercept(null, idMeta.getSetter(), args, null);
	}

	@Test
	public void should_return_simple_property() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, String.class).field("name").accessors()
				.type(PropertyType.SIMPLE).build();

		alreadyLoaded.add(propertyMeta.getGetter());
		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		when(invoker.getValueFromField(target, propertyMeta.getField())).thenReturn(rawValue);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isEqualTo(rawValue);
		verifyZeroInteractions(loader);
	}

	@Test
	public void should_load_and_return_simple_property() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, String.class).field("name").accessors()
				.type(PropertyType.SIMPLE).build();

		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		when(invoker.getValueFromField(target, propertyMeta.getField())).thenReturn(rawValue);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isEqualTo(rawValue);
		verify(loader).loadPropertyIntoObject(context, target, propertyMeta);
	}

	@Test
	public void should_return_list_wrapper() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, String.class).field("friends").accessors()
				.type(PropertyType.LIST).build();

		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		rawValue = new ArrayList<String>();
		when(invoker.getValueFromField(target, propertyMeta.getField())).thenReturn(rawValue);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isInstanceOf(ListWrapper.class);
	}

	@Test
	public void should_return_null_for_list_property() throws Throwable {
		// Given
		Method method = CompleteBean.class.getDeclaredMethod("getFriends");
		Field field = CompleteBean.class.getDeclaredField("friends");
		setterMetas.put(method, pm);
		when(pm.type()).thenReturn(PropertyType.LIST);
		getterMetas.put(method, pm);
		when(invoker.getValueFromField(target, field)).thenReturn(null);

		Object actual = interceptor.intercept(target, method, args, proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_return_set_wrapper() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, String.class).field("followers").accessors()
				.type(PropertyType.SET).build();

		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		rawValue = new HashSet<String>();
		when(invoker.getValueFromField(target, propertyMeta.getField())).thenReturn(rawValue);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isInstanceOf(SetWrapper.class);
	}

	@Test
	public void should_return_null_for_set_property() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, String.class).field("followers").accessors()
				.type(PropertyType.SET).build();

		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		when(proxy.invoke(target, args)).thenReturn(null);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_return_map_wrapper() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Integer.class, String.class).field("preferences").accessors()
				.type(PropertyType.MAP).build();

		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		rawValue = new HashMap<Integer, String>();
		when(invoker.getValueFromField(target, propertyMeta.getField())).thenReturn(rawValue);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isInstanceOf(MapWrapper.class);
	}

	@Test
	public void should_return_null_for_map_property() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Integer.class, String.class).field("preferences").accessors()
				.type(PropertyType.MAP).build();

		getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		when(invoker.getValueFromField(target, propertyMeta.getField())).thenReturn(null);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isNull();
	}

	@Test
	public void should_exception_when_calling_setter_on_counter() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, Counter.class).field("count").accessors()
				.type(PropertyType.COUNTER).build();

		// No setter, use getter to simulate setter
		setterMetas.put(propertyMeta.getGetter(), propertyMeta);
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot set value directly to a Counter type. Please call the getter first to get handle on the wrapper");
		interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

	}

	@Test
	public void should_set_simple_value() throws Throwable {
		// Given
		Method method = CompleteBean.class.getDeclaredMethod("getName");

		setterMetas.put(method, pm);
		when(pm.type()).thenReturn(PropertyType.SIMPLE);

		// When
		Object actual = interceptor.intercept(target, method, new Object[] { rawValue }, proxy);

		// Then
		assertThat(alreadyLoaded).isEmpty();
		assertThat(dirtyMap).containsKey(method);
		assertThat(dirtyMap).containsValue(pm);
		assertThat(actual).isNull();

		verify(pm, times(2)).setValueToField(target, rawValue);
	}

	@Test
	public void should_load_clustered_counter() throws Throwable {
		PropertyMeta propertyMeta = completeBean(Void.class, Counter.class).field("count").accessors()
				.type(PropertyType.COUNTER).invoker(new ReflectionInvoker()).build();

		when(context.isClusteredCounter()).thenReturn(true);
		getterMetas.put(propertyMeta.getGetter(), propertyMeta);

		Object actual = interceptor.intercept(target, propertyMeta.getGetter(), args, proxy);

		assertThat(actual).isNull();
		InternalCounterImpl counter = (InternalCounterImpl) target.getCount();

		assertThat(counter.get()).isNull();
		assertThat(counter.getInternalCounterDelta()).isNull();

		verify(counterLoader).loadClusteredCounterColumn(context, target, propertyMeta);
	}

}
