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

package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.TABLE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.EmbeddedKey;
import net.sf.cglib.proxy.Factory;

public class EntityWithCompositePartitionKeyIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = manager.getNativeSession();

	@Test
	public void should_persist() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.insert(entity);

		Row row = session.execute("SELECT * FROM " + TABLE_NAME + " WHERE id=" + id + " AND type='type'").one();

		assertThat(row).isNotNull();
		assertThat(row.getLong("id")).isEqualTo(id);
		assertThat(row.getString("type")).isEqualTo("type");
		assertThat(row.getString("value")).isEqualTo("value");
	}

	@Test
	public void should_find() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.insert(entity);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));

		assertThat(found).isNotNull();
		assertThat(found).isNotInstanceOf(Factory.class);
		assertThat(found.getId().getId()).isEqualTo(id);
		assertThat(found.getId().getType()).isEqualTo("type");
		assertThat(found.getValue()).isEqualTo("value");
	}

	@Test
	public void should_update_modifications() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.insert(entity);

		final EntityWithCompositePartitionKey proxy = manager.forUpdate(EntityWithCompositePartitionKey.class, entity.getId());

		proxy.setValue("value2");
		manager.update(proxy);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found.getValue()).isEqualTo("value2");
	}

	@Test
	public void should_delete() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.insert(entity);

		manager.delete(entity);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found).isNull();
	}

	@Test
	public void should_delete_by_id() throws Exception {
		long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		EmbeddedKey compositeRowKey = new EmbeddedKey(id, "type");

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "clustered_value");

		manager.insert(entity);

		manager.deleteById(EntityWithCompositePartitionKey.class, compositeRowKey);

		assertThat(manager.find(EntityWithCompositePartitionKey.class, compositeRowKey)).isNull();

	}

}
