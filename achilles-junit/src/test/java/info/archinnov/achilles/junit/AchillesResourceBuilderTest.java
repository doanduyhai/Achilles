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

package info.archinnov.achilles.junit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.test.integration.entity.User;

public class AchillesResourceBuilderTest {

	@Rule
	public AchillesResource resource = AchillesResourceBuilder
			.withEntityPackages("info.archinnov.achilles.test.integration.entity").tablesToTruncate("User")
			.truncateAfterTest().build();

	private PersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();
	private PersistenceManager manager = resource.getPersistenceManager();
	private Session session = resource.getNativeSession();

	@Test
	public void should_bootstrap_embedded_server_and_entity_manager() throws Exception {

		Long id = RandomUtils.nextLong();
		manager.insert(new User(id, "fn", "ln"));

		Row row = session.execute("SELECT * FROM User WHERE id=" + id).one();

		assertThat(row).isNotNull();

		assertThat(row.getString("firstname")).isEqualTo("fn");
		assertThat(row.getString("lastname")).isEqualTo("ln");
	}

	@Test
	public void should_create_resources_once() throws Exception {
		AchillesResource resource = new AchillesResource(null,"info.archinnov.achilles.junit.test.entity");

		assertThat(resource.getPersistenceManagerFactory()).isSameAs(pmf);
		assertThat(resource.getPersistenceManager()).isSameAs(manager);
		assertThat(resource.getNativeSession()).isSameAs(session);
	}

    @Test
    public void should_create_resource_with_a_distinct_keyspace() throws Exception {
        //Given
        AchillesResource resource = AchillesResourceBuilder.noEntityPackages("test_keyspace");
        final PersistenceManager manager = resource.getPersistenceManager();
        RegularStatement regularStatement = select().countAll().from("system","schema_keyspaces")
                .where(eq("keyspace_name","test_keyspace"));

        //When
        final Map<String,Object> map = manager.nativeQuery(regularStatement).first();

        //Then
        assertThat(map.get("count")).isEqualTo(1L);

    }
}
