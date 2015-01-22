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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter.CompoundPK;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter.TABLE_NAME;
import static info.archinnov.achilles.type.CounterBuilder.incr;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter;

public class ClusteredEntityWithCounterIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntityWithCounter entity;

	private CompoundPK compoundKey;

	@Test
	public void should_persist_and_find() throws Exception {
		long counterValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		long versionValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		compoundKey = new CompoundPK(RandomUtils.nextLong(0,Long.MAX_VALUE), "name");

		entity = new ClusteredEntityWithCounter(compoundKey, incr(counterValue), incr(versionValue));

		manager.insert(entity);

		ClusteredEntityWithCounter found = manager.find(ClusteredEntityWithCounter.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getCounter().get()).isEqualTo(counterValue);
		assertThat(found.getVersion().get()).isEqualTo(versionValue);
	}

	@Test
	public void should_persist_and_get_proxy() throws Exception {
		long counterValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		compoundKey = new CompoundPK(RandomUtils.nextLong(0,Long.MAX_VALUE), "name");
		entity = new ClusteredEntityWithCounter(compoundKey, incr(counterValue));

		manager.insert(entity);

		ClusteredEntityWithCounter found = manager.getProxy(ClusteredEntityWithCounter.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getCounter().get()).isEqualTo(counterValue);
		assertThat(found.getVersion().get()).isNull();
	}

	@Test
	public void should_update_modifications() throws Exception {
		long initialValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		long increment = RandomUtils.nextLong(0,Long.MAX_VALUE);

		compoundKey = new CompoundPK(RandomUtils.nextLong(0,Long.MAX_VALUE), "name");

		entity = new ClusteredEntityWithCounter(compoundKey, incr(initialValue),incr(initialValue));

		entity = manager.insert(entity);

        assertThat(entity.getCounter().get()).isEqualTo(initialValue);

		entity.getCounter().incr(increment);
		entity.getVersion().incr(increment);

        manager.update(entity);

		entity = manager.find(ClusteredEntityWithCounter.class, compoundKey);

		assertThat(entity.getCounter().get()).isEqualTo(initialValue + increment);
		assertThat(entity.getVersion().get()).isEqualTo(initialValue + increment);
	}

	@Test
	public void should_delete() throws Exception {
		long counterValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		compoundKey = new CompoundPK(RandomUtils.nextLong(0,Long.MAX_VALUE), "name");

		entity = new ClusteredEntityWithCounter(compoundKey, incr(counterValue));

		entity = manager.insert(entity);

		manager.delete(entity);

		Thread.sleep(2000);

		assertThat(manager.find(ClusteredEntityWithCounter.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {
		long counterValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		long incr = RandomUtils.nextLong(0,Long.MAX_VALUE);

		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		String name = "name";
		compoundKey = new CompoundPK(partitionKey, name);

		entity = new ClusteredEntityWithCounter(compoundKey, incr(counterValue));

		entity = manager.insert(entity);

		session.execute("UPDATE " + TABLE_NAME + " SET counter = counter + " + incr + " WHERE id=" + partitionKey
				+ " AND name='name'");
        session.execute("UPDATE " + TABLE_NAME + " SET version = version + " + incr + " WHERE id=" + partitionKey
                                + " AND name='name'");
		// Wait for the counter to be updated
		Thread.sleep(100);

		manager.refresh(entity);

		assertThat(entity.getCounter().get()).isEqualTo(counterValue + incr);
		assertThat(entity.getVersion().get()).isEqualTo(incr);

	}

	@Test
	public void should_query_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		List<ClusteredEntityWithCounter> entities = manager.sliceQuery(ClusteredEntityWithCounter.class)
                .forSelect()
				.withPartitionComponents(partitionKey).fromClusterings("name2").toClusterings("name4").get();

		assertThat(entities).isEmpty();

		insertValues(partitionKey, 5);

		entities = manager.sliceQuery(ClusteredEntityWithCounter.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
				.fromClusterings("name2")
                .toClusterings("name4").get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getCounter().get()).isEqualTo(2);
		assertThat(entities.get(0).getVersion().get()).isEqualTo(2);
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
		assertThat(entities.get(1).getCounter().get()).isEqualTo(3);
        assertThat(entities.get(1).getVersion().get()).isEqualTo(3);
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getCounter().get()).isEqualTo(4);
        assertThat(entities.get(2).getVersion().get()).isEqualTo(4);
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name4");
	}

	@Test
	public void should_iterate_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		insertValues(partitionKey, 5);

		Iterator<ClusteredEntityWithCounter> iter = manager.sliceQuery(ClusteredEntityWithCounter.class)
                .forIteration()
				.withPartitionComponents(partitionKey)
                .iterator();

		assertThat(iter.hasNext()).isTrue();
		ClusteredEntityWithCounter next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name1");
		assertThat(next.getCounter().get()).isEqualTo(1L);
		assertThat(next.getVersion().get()).isEqualTo(1L);
		assertThat(next.getAnotherCounter().get()).isEqualTo(1L);
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name2");
		assertThat(next.getCounter().get()).isEqualTo(2L);
        assertThat(next.getVersion().get()).isEqualTo(2L);
        assertThat(next.getAnotherCounter().get()).isEqualTo(2L);

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name3");
		assertThat(next.getCounter().get()).isEqualTo(3L);
        assertThat(next.getVersion().get()).isEqualTo(3L);
        assertThat(next.getAnotherCounter().get()).isEqualTo(3L);

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name4");
		assertThat(next.getCounter().get()).isEqualTo(4L);
        assertThat(next.getVersion().get()).isEqualTo(4L);
        assertThat(next.getAnotherCounter().get()).isEqualTo(4L);

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name5");
		assertThat(next.getCounter().get()).isEqualTo(5L);
        assertThat(next.getVersion().get()).isEqualTo(5L);
        assertThat(next.getAnotherCounter().get()).isEqualTo(5L);

		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_delete_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		insertValues(partitionKey, 3);

		manager.sliceQuery(ClusteredEntityWithCounter.class)
                .forDelete()
                .withPartitionComponents(partitionKey)
                .deleteMatching("name2");

		// Wait until counter column is really removed because of absence of
		// tombstone
		Thread.sleep(100);

		List<ClusteredEntityWithCounter> entities = manager.sliceQuery(ClusteredEntityWithCounter.class)
                .forSelect()
				.withPartitionComponents(partitionKey)
                .get(100);

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getCounter().get()).isEqualTo(1L);
		assertThat(entities.get(0).getVersion().get()).isEqualTo(1L);
		assertThat(entities.get(1).getCounter().get()).isEqualTo(3L);
        assertThat(entities.get(1).getVersion().get()).isEqualTo(3L);
	}

	private void insertClusteredEntity(Long partitionKey, String name, Long counterValue) {
		CompoundPK compoundPK = new CompoundPK(partitionKey, name);
		ClusteredEntityWithCounter entity = new ClusteredEntityWithCounter(compoundPK,
				incr(counterValue),incr(counterValue), incr(counterValue));
		manager.insert(entity);
	}

	private void insertValues(long partitionKey, int count) {
		String namePrefix = "name";

		for (int i = 1; i <= count; i++) {
			insertClusteredEntity(partitionKey, namePrefix + i, new Long(i));
		}
	}
}
