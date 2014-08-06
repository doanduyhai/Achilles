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
package info.archinnov.achilles.internal.metadata.util;

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.Test;
import com.google.common.collect.Collections2;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;

public class PropertyTypeExcludeTest {

	@Test
	public void should_exclude_by_types() throws Exception {
		PropertyTypeExclude exclude = new PropertyTypeExclude(COUNTER, SIMPLE);

		PropertyMeta pm1 = PropertyMetaTestBuilder.valueClass(String.class).entityClassName("entity").field("pm1")
				.type(SET).build();

		PropertyMeta pm2 = PropertyMetaTestBuilder.valueClass(String.class).entityClassName("entity").field("pm2")
				.type(SIMPLE).build();

		PropertyMeta pm3 = PropertyMetaTestBuilder.valueClass(String.class).entityClassName("entity").field("pm3")
				.type(MAP).build();

		assertThat(Collections2.filter(Arrays.asList(pm1, pm2), exclude)).containsOnly(pm1);
		assertThat(Collections2.filter(Arrays.asList(pm1, pm3), exclude)).containsOnly(pm1, pm3);
	}
}
