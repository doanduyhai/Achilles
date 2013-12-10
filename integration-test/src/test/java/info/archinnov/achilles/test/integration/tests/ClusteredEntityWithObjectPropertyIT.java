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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.TABLE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.Holder;

public class ClusteredEntityWithObjectPropertyIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private Session session = resource.getNativeSession();

	private ClusteredEntityWithObjectValue entity;

	private ClusteredKey compoundKey;

	private ObjectMapper mapper = new ObjectMapper();

	@Test
	public void should_persist_and_find() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
		Holder holder = new Holder("content");
		entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

		manager.persist(entity);

		ClusteredEntityWithObjectValue found = manager.find(ClusteredEntityWithObjectValue.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo(holder);
	}

	@Test
	public void should_merge_and_get_reference() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
		Holder holder = new Holder("content");
		entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

		manager.merge(entity);

		ClusteredEntityWithObjectValue found = manager.getReference(ClusteredEntityWithObjectValue.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo(holder);
	}

	@Test
	public void should_merge_modifications() throws Exception {

		compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
		Holder holder = new Holder("content");
		Holder newHolder = new Holder("new_content");
		entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

		entity = manager.merge(entity);

		entity.setValue(newHolder);
		manager.merge(entity);

		entity = manager.find(ClusteredEntityWithObjectValue.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo(newHolder);
	}

	@Test
	public void should_remove() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
		Holder holder = new Holder("content");
		entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

		entity = manager.merge(entity);

		manager.remove(entity);

		assertThat(manager.find(ClusteredEntityWithObjectValue.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {

		long partitionKey = RandomUtils.nextLong();
		compoundKey = new ClusteredKey(partitionKey, "name");
		Holder holder = new Holder("content");
		Holder newHolder = new Holder("new_content");

		entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

		entity = manager.merge(entity);

		session.execute("UPDATE " + TABLE_NAME + " SET value='" + mapper.writeValueAsString(newHolder) + "' where id="
				+ partitionKey + " and name='name'");
		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo(newHolder);
	}

	@Test
	public void should_query_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		List<ClusteredEntityWithObjectValue> entities = manager.sliceQuery(ClusteredEntityWithObjectValue.class)
				.partitionComponents(partitionKey).fromClusterings("name2").toClusterings("name4").get();

		assertThat(entities).isEmpty();

		insertValues(partitionKey, 5);

		entities = manager.sliceQuery(ClusteredEntityWithObjectValue.class).partitionComponents(partitionKey)
				.fromClusterings("name2").toClusterings("name4").get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue().getContent()).isEqualTo("name2");
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
		assertThat(entities.get(1).getValue().getContent()).isEqualTo("name3");
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue().getContent()).isEqualTo("name4");
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

		entities = manager.sliceQuery(ClusteredEntityWithObjectValue.class)
				.fromEmbeddedId(new ClusteredKey(partitionKey, "name2"))
				.toEmbeddedId(new ClusteredKey(partitionKey, "name4")).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue().getContent()).isEqualTo("name2");
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
		assertThat(entities.get(1).getValue().getContent()).isEqualTo("name3");
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue().getContent()).isEqualTo("name4");
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name4");
		;
	}

	@Test
	public void should_iterate_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 5);

		Iterator<ClusteredEntityWithObjectValue> iter = manager.sliceQuery(ClusteredEntityWithObjectValue.class)
				.partitionComponents(partitionKey).iterator();

		assertThat(iter.hasNext()).isTrue();
		ClusteredEntityWithObjectValue next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name1");
		assertThat(next.getValue().getContent()).isEqualTo("name1");
		assertThat(iter.hasNext()).isTrue();

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name2");
		assertThat(next.getValue().getContent()).isEqualTo("name2");

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name3");
		assertThat(next.getValue().getContent()).isEqualTo("name3");

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name4");
		assertThat(next.getValue().getContent()).isEqualTo("name4");

		assertThat(iter.hasNext()).isTrue();
		next = iter.next();
		assertThat(next.getId().getId()).isEqualTo(partitionKey);
		assertThat(next.getId().getName()).isEqualTo("name5");
		assertThat(next.getValue().getContent()).isEqualTo("name5");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 3);

		manager.sliceQuery(ClusteredEntityWithObjectValue.class).partitionComponents(partitionKey).fromClusterings("name2")
				.toClusterings("name2").remove();

		List<ClusteredEntityWithObjectValue> entities = manager.sliceQuery(ClusteredEntityWithObjectValue.class)
				.partitionComponents(partitionKey).get(100);

		assertThat(entities).hasSize(2);

		assertThat(entities.get(0).getValue().getContent()).isEqualTo("name1");
		assertThat(entities.get(1).getValue().getContent()).isEqualTo("name3");
	}

	private void insertClusteredEntity(Long partitionKey, String name, Holder clusteredValue) {
		ClusteredKey embeddedId = new ClusteredKey(partitionKey, name);
		ClusteredEntityWithObjectValue entity = new ClusteredEntityWithObjectValue(embeddedId, clusteredValue);
		manager.persist(entity);
	}

	private void insertValues(long partitionKey, int count) {
		String namePrefix = "name";

		for (int i = 1; i <= count; i++) {
			insertClusteredEntity(partitionKey, namePrefix + i, new Holder(namePrefix + i));
		}
	}
}
