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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithReverseClustering.TABLE_NAME;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithReverseClustering;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithReverseClustering.ClusteredKey;

public class ClusteredEntityWithReverseClusteringIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_query_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		List<ClusteredEntityWithReverseClustering> entities = manager
				.sliceQuery(ClusteredEntityWithReverseClustering.class).partitionComponents(partitionKey).getFirst(5);
		assertThat(entities).isEmpty();

		insertValues(partitionKey, 5);

		entities = manager.sliceQuery(ClusteredEntityWithReverseClustering.class).partitionComponents(partitionKey)
				.fromClusterings(4).toClusterings(2).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo("value4");
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getCount()).isEqualTo(4);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name4");
		assertThat(entities.get(1).getValue()).isEqualTo("value3");
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getCount()).isEqualTo(3);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue()).isEqualTo("value2");
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getCount()).isEqualTo(2);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name2");

		entities = manager.sliceQuery(ClusteredEntityWithReverseClustering.class)
				.fromEmbeddedId(new ClusteredKey(partitionKey, 4, null))
				.toEmbeddedId(new ClusteredKey(partitionKey, 2, null)).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo("value4");
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getCount()).isEqualTo(4);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name4");
		assertThat(entities.get(1).getValue()).isEqualTo("value3");
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getCount()).isEqualTo(3);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue()).isEqualTo("value2");
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getCount()).isEqualTo(2);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name2");
	}

	@Test
	public void should_query_with_reverse_ordering() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 5);

		List<ClusteredEntityWithReverseClustering> entities = manager
				.sliceQuery(ClusteredEntityWithReverseClustering.class).partitionComponents(partitionKey)
				.fromClusterings(2).toClusterings(4).ordering(DESCENDING).get();

		assertThat(entities).hasSize(3);

		assertThat(entities.get(0).getValue()).isEqualTo("value2");
		assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(0).getId().getCount()).isEqualTo(2);
		assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
		assertThat(entities.get(1).getValue()).isEqualTo("value3");
		assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(1).getId().getCount()).isEqualTo(3);
		assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
		assertThat(entities.get(2).getValue()).isEqualTo("value4");
		assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
		assertThat(entities.get(2).getId().getCount()).isEqualTo(4);
		assertThat(entities.get(2).getId().getName()).isEqualTo("name4");
	}

	private void insertValues(long partitionKey, int size) {
		String namePrefix = "name";
		String clusteredValuePrefix = "value";

		for (int i = 1; i <= size; i++) {
			insertClusteredEntity(partitionKey, i, namePrefix + i, clusteredValuePrefix + i);
		}
	}

	private void insertClusteredEntity(Long partitionKey, int count, String name, String clusteredValue) {
		ClusteredKey embeddedId = new ClusteredKey(partitionKey, count, name);
		ClusteredEntityWithReverseClustering entity = new ClusteredEntityWithReverseClustering(embeddedId,
				clusteredValue);
		manager.persist(entity);
	}
}
