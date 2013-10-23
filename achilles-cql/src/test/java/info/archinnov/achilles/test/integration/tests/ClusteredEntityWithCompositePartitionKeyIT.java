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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCompositePartitionKey.*;
import static info.archinnov.achilles.type.BoundingMode.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCompositePartitionKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCompositePartitionKey.EmbeddedKey;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class ClusteredEntityWithCompositePartitionKeyIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntityWithCompositePartitionKey entity;

	private EmbeddedKey compoundKey;

	@Test
	public void should_persist_and_find() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index = 11;
		compoundKey = new EmbeddedKey(id, "type", index);

		entity = new ClusteredEntityWithCompositePartitionKey(id, "type", index, "value");

		manager.persist(entity);

		ClusteredEntityWithCompositePartitionKey found = manager.find(ClusteredEntityWithCompositePartitionKey.class,
				compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("value");
	}

	@Test
	public void should_merge_and_get_reference() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index = 11;
		compoundKey = new EmbeddedKey(id, "type", index);

		entity = new ClusteredEntityWithCompositePartitionKey(id, "type", index, "clustered_value");

		manager.merge(entity);

		ClusteredEntityWithCompositePartitionKey found = manager.getReference(
				ClusteredEntityWithCompositePartitionKey.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_modifications() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index = 11;
		compoundKey = new EmbeddedKey(id, "type", index);

		entity = new ClusteredEntityWithCompositePartitionKey(id, "type", index, "clustered_value");

		entity = manager.merge(entity);

		entity.setValue("new_clustered_value");
		manager.merge(entity);

		entity = manager.find(ClusteredEntityWithCompositePartitionKey.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}

	@Test
	public void should_remove() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index = 11;
		compoundKey = new EmbeddedKey(id, "type", index);

		entity = new ClusteredEntityWithCompositePartitionKey(id, "type", index, "clustered_value");

		entity = manager.merge(entity);

		manager.remove(entity);

		assertThat(manager.find(ClusteredEntityWithCompositePartitionKey.class, compoundKey)).isNull();

	}

	@Test
	public void should_remove_by_id() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index = 11;
		compoundKey = new EmbeddedKey(id, "type", index);

		entity = new ClusteredEntityWithCompositePartitionKey(id, "type", index, "clustered_value");

		entity = manager.merge(entity);

		manager.removeById(ClusteredEntityWithCompositePartitionKey.class, entity.getId());

		assertThat(manager.find(ClusteredEntityWithCompositePartitionKey.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index = 11;
		compoundKey = new EmbeddedKey(id, "type", index);

		entity = new ClusteredEntityWithCompositePartitionKey(id, "type", index, "clustered_value");

		entity = manager.merge(entity);

		session.execute("UPDATE " + TABLE_NAME + " SET value='new_clustered_value' WHERE id=" + id
				+ " AND type='type' AND indexes=11");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");

	}

	@Test
	public void should_query_with_default_params() throws Exception {
		long id = RandomUtils.nextLong();
		Integer index1 = 10;
		Integer index2 = 12;
		List<ClusteredEntityWithCompositePartitionKey> entities = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.fromClusterings(index1).toClusterings(index2).get();

		assertThat(entities).isEmpty();

		insertValues(id, 5);

		entities = manager.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.fromClusterings(index1).toClusterings(index2).get();

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getValue()).isEqualTo("value1");
		assertThat(entities.get(0).getId().getId()).isEqualTo(id);
		assertThat(entities.get(0).getId().getType()).isEqualTo("type");
		assertThat(entities.get(0).getId().getIndexes()).isEqualTo(11);

		assertThat(entities.get(1).getValue()).isEqualTo("value2");
		assertThat(entities.get(1).getId().getId()).isEqualTo(id);
		assertThat(entities.get(1).getId().getType()).isEqualTo("type");
		assertThat(entities.get(1).getId().getIndexes()).isEqualTo(12);
	}

	@Test
	public void should_check_for_common_operation_on_found_clustered_entity() throws Exception {
		long id = RandomUtils.nextLong();
		insertValues(id, 1);

		ClusteredEntityWithCompositePartitionKey clusteredEntity = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.getFirstOccurence();

		// Check for merge
		clusteredEntity.setValue("dirty");
		manager.merge(clusteredEntity);

		ClusteredEntityWithCompositePartitionKey check = manager.find(ClusteredEntityWithCompositePartitionKey.class,
				clusteredEntity.getId());
		assertThat(check.getValue()).isEqualTo("dirty");

		// Check for refresh
		check.setValue("dirty_again");
		manager.merge(check);

		manager.refresh(clusteredEntity);
		assertThat(clusteredEntity.getValue()).isEqualTo("dirty_again");

		// Check for remove
		manager.remove(clusteredEntity);
		assertThat(manager.find(ClusteredEntityWithCompositePartitionKey.class, clusteredEntity.getId())).isNull();
	}

	@Test
	public void should_query_with_custom_params() throws Exception {
		long id = RandomUtils.nextLong();
		insertValues(id, 5);

		List<ClusteredEntityWithCompositePartitionKey> entities = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.fromClusterings(14).toClusterings(11).bounding(INCLUSIVE_END_BOUND_ONLY).ordering(DESCENDING).limit(2)
				.get();

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getValue()).isEqualTo("value3");
		assertThat(entities.get(1).getValue()).isEqualTo("value2");

		entities = manager.sliceQuery(ClusteredEntityWithCompositePartitionKey.class)
				.fromEmbeddedId(new EmbeddedKey(id, "type", 14)).toEmbeddedId(new EmbeddedKey(id, "type", 11))
				.bounding(INCLUSIVE_END_BOUND_ONLY).ordering(DESCENDING).limit(4).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo("value3");
		assertThat(entities.get(1).getValue()).isEqualTo("value2");
		assertThat(entities.get(2).getValue()).isEqualTo("value1");

	}

	@Test
	public void should_query_with_consistency_level() throws Exception {
		Long id = RandomUtils.nextLong();
		insertValues(id, 5);

		exception.expect(InvalidQueryException.class);
		exception.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

		manager.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").fromClusterings(12)
				.toClusterings(14).consistencyLevel(EACH_QUORUM).get();
	}

	@Test
	public void should_query_with_getFirst() throws Exception {
		long id = RandomUtils.nextLong();
		ClusteredEntityWithCompositePartitionKey entity = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.getFirstOccurence();

		assertThat(entity).isNull();

		insertValues(id, 5);

		entity = manager.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.getFirstOccurence();

		assertThat(entity.getValue()).isEqualTo("value1");

		List<ClusteredEntityWithCompositePartitionKey> entities = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").getFirst(3);

		assertThat(entities).hasSize(3);
		assertThat(entities.get(0).getValue()).isEqualTo("value1");
		assertThat(entities.get(1).getValue()).isEqualTo("value2");
		assertThat(entities.get(2).getValue()).isEqualTo("value3");

	}

	@Test
	public void should_query_with_getLast() throws Exception {
		long id = RandomUtils.nextLong();

		ClusteredEntityWithCompositePartitionKey entity = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").getLastOccurence();

		assertThat(entity).isNull();

		insertValues(id, 5);

		entity = manager.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.getLastOccurence();

		assertThat(entity.getValue()).isEqualTo("value5");

		List<ClusteredEntityWithCompositePartitionKey> entities = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").getLast(3);

		assertThat(entities).hasSize(3);
		assertThat(entities.get(0).getValue()).isEqualTo("value5");
		assertThat(entities.get(1).getValue()).isEqualTo("value4");
		assertThat(entities.get(2).getValue()).isEqualTo("value3");
	}

	@Test
	public void should_iterate_with_default_params() throws Exception {
		long id = RandomUtils.nextLong();
		insertValues(id, 5);

		Iterator<ClusteredEntityWithCompositePartitionKey> iter = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").iterator();

		assertThat(iter.hasNext()).isTrue();
		ClusteredEntityWithCompositePartitionKey next = iter.next();
		assertThat(next.getValue()).isEqualTo("value1");
		assertThat(next.getId().getId()).isEqualTo(id);
		assertThat(next.getId().getType()).isEqualTo("type");
		assertThat(next.getId().getIndexes()).isEqualTo(11);
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getValue()).isEqualTo("value2");
		assertThat(next.getId().getId()).isEqualTo(id);
		assertThat(next.getId().getType()).isEqualTo("type");
		assertThat(next.getId().getIndexes()).isEqualTo(12);
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getValue()).isEqualTo("value3");
		assertThat(next.getId().getId()).isEqualTo(id);
		assertThat(next.getId().getType()).isEqualTo("type");
		assertThat(next.getId().getIndexes()).isEqualTo(13);
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getValue()).isEqualTo("value4");
		assertThat(next.getId().getId()).isEqualTo(id);
		assertThat(next.getId().getType()).isEqualTo("type");
		assertThat(next.getId().getIndexes()).isEqualTo(14);
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getValue()).isEqualTo("value5");
		assertThat(next.getId().getId()).isEqualTo(id);
		assertThat(next.getId().getType()).isEqualTo("type");
		assertThat(next.getId().getIndexes()).isEqualTo(15);

		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_check_for_common_operation_on_found_clustered_entity_by_iterator() throws Exception {
		long id = RandomUtils.nextLong();
		insertValues(id, 1);

		Iterator<ClusteredEntityWithCompositePartitionKey> iter = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").iterator();

		iter.hasNext();
		ClusteredEntityWithCompositePartitionKey clusteredEntity = iter.next();

		// Check for merge
		clusteredEntity.setValue("dirty");
		manager.merge(clusteredEntity);

		ClusteredEntityWithCompositePartitionKey check = manager.find(ClusteredEntityWithCompositePartitionKey.class,
				clusteredEntity.getId());
		assertThat(check.getValue()).isEqualTo("dirty");

		// Check for refresh
		check.setValue("dirty_again");
		manager.merge(check);

		manager.refresh(clusteredEntity);
		assertThat(clusteredEntity.getValue()).isEqualTo("dirty_again");

		// Check for remove
		manager.remove(clusteredEntity);
		assertThat(manager.find(ClusteredEntityWithCompositePartitionKey.class, clusteredEntity.getId())).isNull();
	}

	@Test
	public void should_iterate_with_custom_params() throws Exception {
		long id = RandomUtils.nextLong();
		insertValues(id, 5);

		Iterator<ClusteredEntityWithCompositePartitionKey> iter = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type")
				.fromClusterings(12).iterator(2);

		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo("value2");
		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo("value4");
		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo("value5");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_with_default_params() throws Exception {
		long id = RandomUtils.nextLong();
		insertValues(id, 3);

		manager.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").fromClusterings(12)
				.toClusterings(12).remove();

		List<ClusteredEntityWithCompositePartitionKey> entities = manager
				.sliceQuery(ClusteredEntityWithCompositePartitionKey.class).partitionComponents(id, "type").get(100);

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getValue()).isEqualTo("value1");
		assertThat(entities.get(1).getValue()).isEqualTo("value3");
	}

	private void insertValues(long id, int count) {
		for (int i = 1; i <= count; i++) {
			insertClusteredEntity(id, 10 + i, "value" + i);
		}
	}

	private void insertClusteredEntity(Long id, Integer index, String clusteredValue) {
		ClusteredEntityWithCompositePartitionKey entity = new ClusteredEntityWithCompositePartitionKey(id, "type",
				index, clusteredValue);
		manager.persist(entity);
	}
}
