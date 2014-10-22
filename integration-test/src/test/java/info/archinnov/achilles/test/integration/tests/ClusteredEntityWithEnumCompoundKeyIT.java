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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.TABLE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;

public class ClusteredEntityWithEnumCompoundKeyIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntityWithEnumCompoundKey entity;

	private ClusteredKey compoundKey;

	@Test
	public void should_persist_and_get_reference() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), Type.AUDIO);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		manager.insert(entity);

		ClusteredEntityWithEnumCompoundKey found = manager.getProxy(ClusteredEntityWithEnumCompoundKey.class,
                                                                    compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_persist_and_find() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), Type.AUDIO);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		manager.insert(entity);

		ClusteredEntityWithEnumCompoundKey found = manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_update_modifications() throws Exception {

		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), Type.FILE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		entity.setValue("new_clustered_value");
		manager.update(entity);

		entity = manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}

	@Test
	public void should_remove() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(0,Long.MAX_VALUE), Type.IMAGE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		manager.delete(entity);

		assertThat(manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {

		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		compoundKey = new ClusteredKey(partitionKey, Type.FILE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.insert(entity);

		session.execute("UPDATE " + TABLE_NAME + " set value='new_clustered_value' where id=" + partitionKey
				+ " and type = 'FILE'");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}
}
