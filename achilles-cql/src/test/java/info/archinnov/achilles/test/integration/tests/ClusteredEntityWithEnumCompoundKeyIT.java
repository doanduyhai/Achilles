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
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;

public class ClusteredEntityWithEnumCompoundKeyIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntityWithEnumCompoundKey entity;

	private ClusteredKey compoundKey;

	@Test
	public void should_persist_and_get_reference() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.AUDIO);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		manager.persist(entity);

		ClusteredEntityWithEnumCompoundKey found = manager.getReference(ClusteredEntityWithEnumCompoundKey.class,
				compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_and_find() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.AUDIO);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		manager.merge(entity);

		ClusteredEntityWithEnumCompoundKey found = manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_modifications() throws Exception {

		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.FILE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		entity.setValue("new_clustered_value");
		manager.merge(entity);

		entity = manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}

	@Test
	public void should_remove() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.IMAGE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		manager.remove(entity);

		assertThat(manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {

		long partitionKey = RandomUtils.nextLong();
		compoundKey = new ClusteredKey(partitionKey, Type.FILE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		session.execute("UPDATE " + TABLE_NAME + " set value='new_clustered_value' where id=" + partitionKey
				+ " and type = 'FILE'");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}
}
