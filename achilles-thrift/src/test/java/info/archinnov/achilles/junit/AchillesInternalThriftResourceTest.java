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

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManagerFactory;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.User;

import java.util.List;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

public class AchillesInternalThriftResourceTest {

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(Steps.AFTER_TEST, "User");

	private Cluster cluster = resource.getCluster();
	private Keyspace keyspace = resource.getKeyspace();
	private ThriftPersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();
	private ThriftPersistenceManager manager = resource.getPersistenceManager();
	private ThriftConsistencyLevelPolicy policy = resource.getConsistencyPolicy();
	private ThriftGenericEntityDao dao = resource.getEntityDao("User", Long.class);

	@Test
	public void should_bootstrap_embedded_server_and_entity_manager() throws Exception {

		Long id = RandomUtils.nextLong();
		manager.persist(new User(id, "fn", "ln"));

		List<Pair<Composite, Object>> columnsRange = dao.findColumnsRange(id, null, null, false, 100);

		assertThat(columnsRange).hasSize(3);

		String idName = columnsRange.get(0).left.get(1, STRING_SRZ);
		String idValue = (String) columnsRange.get(0).right;

		String firstnameName = columnsRange.get(1).left.get(1, STRING_SRZ);
		String firstnameValue = (String) columnsRange.get(1).right;

		String lastnameName = columnsRange.get(2).left.get(1, STRING_SRZ);
		String lastnameValue = (String) columnsRange.get(2).right;

		assertThat(idName).isEqualTo("id");
		assertThat(idValue).isEqualTo(id.toString());

		assertThat(firstnameName).isEqualTo("firstname");
		assertThat(firstnameValue).isEqualTo("fn");

		assertThat(lastnameName).isEqualTo("lastname");
		assertThat(lastnameValue).isEqualTo("ln");
	}

	@Test
	public void should_create_resources_once() throws Exception {
		AchillesInternalThriftResource resource = new AchillesInternalThriftResource();

		assertThat(resource.getCluster()).isSameAs(cluster);
		assertThat(resource.getKeyspace()).isSameAs(keyspace);
		assertThat(resource.getPersistenceManagerFactory()).isSameAs(pmf);
		assertThat(resource.getPersistenceManager()).isSameAs(manager);
		assertThat(resource.getConsistencyPolicy()).isSameAs(policy);
	}
}
