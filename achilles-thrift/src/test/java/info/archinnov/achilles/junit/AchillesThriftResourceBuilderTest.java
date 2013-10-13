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

package info.archinnov.achilles.junit;

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManagerFactory;
import info.archinnov.achilles.test.integration.entity.User;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

public class AchillesThriftResourceBuilderTest {

	@Rule
	public AchillesThriftResource resource = AchillesThriftResourceBuilder
			.withEntityPackages("info.archinnov.achilles.test.integration.entity").tablesToTruncate("User")
			.truncateAfterTest().build();

	private ThriftPersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();
	private ThriftPersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_bootstrap_embedded_server_and_entity_manager() throws Exception {

		Long id = RandomUtils.nextLong();
		manager.persist(new User(id, "fn", "ln"));
		assertThat(manager.find(User.class, id)).isNotNull();
		manager.removeById(User.class, id);
		assertThat(manager.find(User.class, id)).isNull();
	}

	@Test
	public void should_create_resources_once() throws Exception {
		AchillesThriftResource resource = new AchillesThriftResource("info.archinnov.achilles.junit.test.entity");

		assertThat(resource.getPersistenceManagerFactory()).isSameAs(pmf);
		assertThat(resource.getPersistenceManager()).isSameAs(manager);
	}
}
