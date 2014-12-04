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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity.ClusteredKey;
import info.archinnov.achilles.type.OptionsBuilder;

public class ClusteredEntityIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntity entity;

	private ClusteredKey compoundKey;

	@Test
	public void should_persist_and_find() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextInt(0,Integer.MAX_VALUE), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		manager.insert(entity);

		ClusteredEntity found = manager.find(ClusteredEntity.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_persist_with_ttl() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextInt(0,Integer.MAX_VALUE), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		manager.insert(entity, OptionsBuilder.withTtl(1));

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNotNull();

		Thread.sleep(1000);

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();
	}

	@Test
	public void should_update_with_ttl() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextInt(0,Integer.MAX_VALUE), "name");
		entity = new ClusteredEntity(compoundKey, "clustered_value");
		entity = manager.insert(entity, OptionsBuilder.withTtl(1));

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNotNull();

		Thread.sleep(1000);

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();
	}

	@Test
	public void should_update_modifications() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextInt(0,Integer.MAX_VALUE), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		entity.setValue("new_clustered_value");
		manager.update(entity);

		entity = manager.find(ClusteredEntity.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}

	@Test
	public void should_delete() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextInt(0,Integer.MAX_VALUE), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		manager.delete(entity);

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();

	}

	@Test
	public void should_delete_by_id() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), RandomUtils.nextInt(0,Integer.MAX_VALUE), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		manager.deleteById(ClusteredEntity.class, entity.getId());

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {

		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		int count = RandomUtils.nextInt(0,Integer.MAX_VALUE);
		String name = "name";
		compoundKey = new ClusteredKey(partitionKey, count, name);

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		session.execute("update " + TABLE_NAME + " set value='new_clustered_value' where id=" + partitionKey
				+ " and count=" + count + " and name='" + name + "'");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");

	}


}
