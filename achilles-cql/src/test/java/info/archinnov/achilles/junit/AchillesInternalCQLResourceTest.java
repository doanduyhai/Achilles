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

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.User;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class AchillesInternalCQLResourceTest {
	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "User");

	private CQLEntityManagerFactory emf = resource.getFactory();
	private CQLEntityManager em = resource.getEm();
	private Session session = resource.getNativeSession();

	@Test
	public void should_bootstrap_embedded_server_and_entity_manager() throws Exception {

		Long id = RandomUtils.nextLong();
		em.persist(new User(id, "fn", "ln"));

		Row row = session.execute("SELECT * FROM User WHERE id=" + id).one();

		assertThat(row).isNotNull();

		assertThat(row.getString("firstname")).isEqualTo("fn");
		assertThat(row.getString("lastname")).isEqualTo("ln");
	}

	@Test
	public void should_create_resources_once() throws Exception {
		AchillesInternalCQLResource resource = new AchillesInternalCQLResource();

		assertThat(resource.getFactory()).isSameAs(emf);
		assertThat(resource.getEm()).isSameAs(em);
		assertThat(resource.getNativeSession()).isSameAs(session);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null_entity_package_provided() throws Exception {
		new AchillesCQLResource(null);
	}
}
