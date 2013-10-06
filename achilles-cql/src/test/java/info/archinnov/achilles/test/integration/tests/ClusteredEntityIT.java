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

import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity.ClusteredKey;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class ClusteredEntityIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "clustered");

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntity entity;

	private ClusteredKey compoundKey;

	@Test
	public void should_persist_and_find() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		manager.persist(entity);

		ClusteredEntity found = manager.find(ClusteredEntity.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_persist_with_ttl() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		manager.persist(entity, OptionsBuilder.withTtl(2));

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNotNull();

		Thread.sleep(2000);

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();
	}

	@Test
	public void should_merge_and_get_reference() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		manager.merge(entity);

		ClusteredEntity found = manager.getReference(ClusteredEntity.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_with_ttl() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");
		entity = new ClusteredEntity(compoundKey, "clustered_value");
		entity = manager.merge(entity, OptionsBuilder.withTtl(2));

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNotNull();

		Thread.sleep(2000);

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();
	}

	@Test
	public void should_merge_modifications() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		entity.setValue("new_clustered_value");
		manager.merge(entity);

		entity = manager.find(ClusteredEntity.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}

	@Test
	public void should_remove() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		manager.remove(entity);

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();

	}

	@Test
	public void should_remove_by_id() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), RandomUtils.nextInt(), "name");

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		manager.removeById(ClusteredEntity.class, entity.getId());

		assertThat(manager.find(ClusteredEntity.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {

		long partitionKey = RandomUtils.nextLong();
		int count = RandomUtils.nextInt();
		String name = "name";
		compoundKey = new ClusteredKey(partitionKey, count, name);

		entity = new ClusteredEntity(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		session.execute("update clustered set value='new_clustered_value' where id=" + partitionKey + " and count="
				+ count + " and name='" + name + "'");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");

	}

	@Test
	public void should_query_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey)
				.fromClusterings(1, "name2").toClusterings(1, "name4").get();

		assertThat(entities).isEmpty();

		String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

		entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).fromClusterings(1, "name2")
				.toClusterings(1, "name4").get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 2);
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 3);
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 4);
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

		entities = manager.sliceQuery(ClusteredEntity.class).fromEmbeddedId(new ClusteredKey(partitionKey, 1, "name2"))
				.toEmbeddedId(new ClusteredKey(partitionKey, 1, "name4")).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 2);
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 3);
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 4);
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name4");
	}

	@Test
	public void should_check_for_common_operation_on_found_clustered_entity() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 1, 1);

		ClusteredEntity clusteredEntity = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey)
				.getFirstOccurence();

		// Check for merge
		clusteredEntity.setValue("dirty");
		manager.merge(clusteredEntity);

		ClusteredEntity check = manager.find(ClusteredEntity.class, clusteredEntity.getId());
		assertThat(check.getValue()).isEqualTo("dirty");

		// Check for refresh
		check.setValue("dirty_again");
		manager.merge(check);

		manager.refresh(clusteredEntity);
		assertThat(clusteredEntity.getValue()).isEqualTo("dirty_again");

		// Check for remove
		manager.remove(clusteredEntity);
		assertThat(manager.find(ClusteredEntity.class, clusteredEntity.getId())).isNull();
	}

	@Test
	public void should_query_with_custom_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

		List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey)
				.fromClusterings(1, "name4").toClusterings(1, "name1").bounding(INCLUSIVE_END_BOUND_ONLY)
				.ordering(DESCENDING).limit(2).get();

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 3);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);

		entities = manager.sliceQuery(ClusteredEntity.class).fromEmbeddedId(new ClusteredKey(partitionKey, 1, "name4"))
				.toEmbeddedId(new ClusteredKey(partitionKey, 1, "name1")).bounding(INCLUSIVE_END_BOUND_ONLY)
				.ordering(DESCENDING).limit(4).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 3);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 1);

	}

	@Test
	public void should_query_with_consistency_level() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 1, 5);

		exception.expect(InvalidQueryException.class);
		exception.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

		manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).fromClusterings(1, "name2")
				.toClusterings(1, "name4").consistencyLevel(EACH_QUORUM).get();
	}

	@Test
	public void should_query_with_getFirst() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getFirstOccurence();

		assertThat(entity).isNull();

		String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

		entity = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getFirstOccurence();

		assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 1);

		entity = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getFirstOccurence();

		assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 1);

		List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getFirst(3);

		assertThat(entities).hasSize(3);
		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

		insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
		insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);

		entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getFirst(3, 4);

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 41);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 42);

	}

	@Test
	public void should_query_with_getLast() throws Exception {
		long partitionKey = RandomUtils.nextLong();

		ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getLastOccurence();

		assertThat(entity).isNull();

		String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

		entity = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getLastOccurence();

		assertThat(entity.getValue()).isEqualTo(clusteredValuePrefix + 5);

		List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getLast(3);

		assertThat(entities).hasSize(3);
		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 5);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 4);
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 3);

		insertClusteredEntity(partitionKey, 4, "name41", clusteredValuePrefix + 41);
		insertClusteredEntity(partitionKey, 4, "name42", clusteredValuePrefix + 42);
		insertClusteredEntity(partitionKey, 4, "name43", clusteredValuePrefix + 43);
		insertClusteredEntity(partitionKey, 4, "name44", clusteredValuePrefix + 44);

		entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).getLast(3, 4);

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 44);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 43);
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 42);

	}

	@Test
	public void should_iterate_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

		Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).iterator();

		assertThat(iter.hasNext()).isTrue();
		ClusteredEntity next = iter.next();
		assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 1);
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getCount()).isEqualTo(1);
		assertThat(next.getId().getName()).isEqualTo("name1");
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getCount()).isEqualTo(1);
		assertThat(next.getId().getName()).isEqualTo("name2");
		assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 2);

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getCount()).isEqualTo(1);
		assertThat(next.getId().getName()).isEqualTo("name3");
		assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 3);

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getCount()).isEqualTo(1);
		assertThat(next.getId().getName()).isEqualTo("name4");
		assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 4);

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getCount()).isEqualTo(1);
		assertThat(next.getId().getName()).isEqualTo("name5");
		assertThat(next.getValue()).isEqualTo(clusteredValuePrefix + 5);
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_check_for_common_operation_on_found_clustered_entity_by_iterator() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 1, 1);

		Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).iterator();

		iter.hasNext();
		ClusteredEntity clusteredEntity = iter.next();

		// Check for merge
		clusteredEntity.setValue("dirty");
		manager.merge(clusteredEntity);

		ClusteredEntity check = manager.find(ClusteredEntity.class, clusteredEntity.getId());
		assertThat(check.getValue()).isEqualTo("dirty");

		// Check for refresh
		check.setValue("dirty_again");
		manager.merge(check);

		manager.refresh(clusteredEntity);
		assertThat(clusteredEntity.getValue()).isEqualTo("dirty_again");

		// Check for remove
		manager.remove(clusteredEntity);
		assertThat(manager.find(ClusteredEntity.class, clusteredEntity.getId())).isNull();
	}

	@Test
	public void should_iterate_with_custom_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		String clusteredValuePrefix = insertValues(partitionKey, 1, 5);

		Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey)
				.fromClusterings(1, "name2").toClusterings(1).iterator(2);

		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 2);
		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 3);
		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 4);
		assertThat(iter.hasNext()).isTrue();
		assertThat(iter.next().getValue()).isEqualTo(clusteredValuePrefix + 5);
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		String clusteredValuePrefix = insertValues(partitionKey, 1, 2);
		insertValues(partitionKey, 2, 3);
		insertValues(partitionKey, 3, 1);

		manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).fromClusterings(2).toClusterings(2).remove();

		List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).get(100);

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(0).getValue()).isEqualTo(clusteredValuePrefix + 1);
		assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
		assertThat(entities.get(1).getValue()).isEqualTo(clusteredValuePrefix + 2);

		assertThat(entities.get(2).getId().getCount()).isEqualTo(3);
		assertThat(entities.get(2).getValue()).isEqualTo(clusteredValuePrefix + 1);
	}

	@Test
	public void should_exception_when_remove_with_varying_components() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 1, 5);

		exception.expect(AchillesException.class);
		exception.expectMessage("CQL does not support slice delete with varying compound components");

		manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).fromClusterings(1).toClusterings(1, "name2")
				.ordering(DESCENDING).limit(2).remove();

	}

	@Test
	public void should_exception_when_remove_with_limit() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 1, 5);

		exception.expect(AchillesException.class);
		exception.expectMessage("CQL slice delete does not support LIMIT");

		manager.sliceQuery(ClusteredEntity.class).partitionKey(partitionKey).remove(3);

	}

	private String insertValues(long partitionKey, int countValue, int size) {
		String namePrefix = "name";
		String clusteredValuePrefix = "value";

		for (int i = 1; i <= size; i++) {
			insertClusteredEntity(partitionKey, countValue, namePrefix + i, clusteredValuePrefix + i);
		}
		return clusteredValuePrefix;
	}

	private void insertClusteredEntity(Long partitionKey, int count, String name, String clusteredValue) {
		ClusteredKey embeddedId = new ClusteredKey(partitionKey, count, name);
		ClusteredEntity entity = new ClusteredEntity(embeddedId, clusteredValue);
		manager.persist(entity);
	}
}
