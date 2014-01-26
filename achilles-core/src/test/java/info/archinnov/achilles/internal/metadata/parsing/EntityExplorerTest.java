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
package info.archinnov.achilles.internal.metadata.parsing;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import info.archinnov.achilles.internal.metadata.parsing.EntityExplorer;
import info.archinnov.achilles.test.more.entity.Entity3;
import info.archinnov.achilles.test.sample.entity.Entity1;
import info.archinnov.achilles.test.sample.entity.Entity2;

public class EntityExplorerTest {
	private EntityExplorer explorer = new EntityExplorer();

	@Test
	public void should_find_entities_from_multiple_packages() throws Exception {
		List<Class<?>> entities = explorer.discoverEntities(Arrays.asList("info.archinnov.achilles.test.sample.entity",
				"info.archinnov.achilles.test.more.entity"));

		assertThat(entities).hasSize(3);
		assertThat(entities).contains(Entity1.class);
		assertThat(entities).contains(Entity2.class);
		assertThat(entities).contains(Entity3.class);
	}

	@Test
	public void should_find_entity_from_one_package() throws Exception {
		List<Class<?>> entities = explorer.discoverEntities(Arrays.asList("info.archinnov.achilles.test.more.entity"));
		assertThat(entities).hasSize(1);
		assertThat(entities).contains(Entity3.class);

	}

}
