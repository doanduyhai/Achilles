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
package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

@RunWith(MockitoJUnitRunner.class)
public class AbstractWrapperTest {
	@Mock
	private AbstractWrapper wrapper;

	private Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();

	private PropertyMeta propertyMeta;

	@Before
	public void setUp() throws Exception {
		dirtyMap.clear();
		doCallRealMethod().when(wrapper).setDirtyMap(dirtyMap);
		wrapper.setDirtyMap(dirtyMap);

		propertyMeta = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(PropertyType.SIMPLE).build();

		doCallRealMethod().when(wrapper).setPropertyMeta(propertyMeta);
		wrapper.setPropertyMeta(propertyMeta);

		doCallRealMethod().when(wrapper).setSetter(propertyMeta.getSetter());
		wrapper.setSetter(propertyMeta.getSetter());
	}

	@Test
	public void should_mark_dirty() throws Exception {
		doCallRealMethod().when(wrapper).markDirty();
		wrapper.markDirty();

		assertThat(dirtyMap).containsKey(propertyMeta.getSetter());
		assertThat(dirtyMap).containsValue(propertyMeta);
	}

	@Test
	public void should_not_mark_dirty_if_already_dirty() throws Exception {
		dirtyMap.put(propertyMeta.getSetter(), propertyMeta);
		doCallRealMethod().when(wrapper).markDirty();

		wrapper.markDirty();

		assertThat(dirtyMap).hasSize(1);
		assertThat(dirtyMap).containsValue(propertyMeta);
	}
}
