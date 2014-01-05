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
package info.archinnov.achilles.entity.metadata.util;

import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

@RunWith(MockitoJUnitRunner.class)
public class AlreadyLoadedTransformerTest {

	@Test
	public void should_transform() throws Exception {
		Map<Method, PropertyMeta> getterMetas = new HashMap<Method, PropertyMeta>();
		AlreadyLoadedTransformer transformer = new AlreadyLoadedTransformer(getterMetas);

		PropertyMeta pm1 = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(PropertyType.SIMPLE).build();

		getterMetas.put(pm1.getGetter(), pm1);

		List<PropertyMeta> list = FluentIterable.from(Arrays.asList(pm1.getGetter())).transform(transformer)
				.toImmutableList();

		assertThat(list).containsExactly(pm1);
	}
}
