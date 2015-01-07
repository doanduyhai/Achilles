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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithReverseClustering.CompoundPK;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithReverseClustering.TABLE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithReverseClustering;

public class ClusteredEntityWithReverseClusteringIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_query_with_default_params() throws Exception {
		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        List<ClusteredEntityWithReverseClustering> entities = manager
				.sliceQuery(ClusteredEntityWithReverseClustering.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByAscending()
                .get(5);

		assertThat(entities).isEmpty();

		insertValues(partitionKey, 5);

		entities = manager.sliceQuery(ClusteredEntityWithReverseClustering.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
				.fromClusterings(4)
                .toClusterings(2)
                .get();

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
		long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
		insertValues(partitionKey, 5);

		List<ClusteredEntityWithReverseClustering> entities = manager
				.sliceQuery(ClusteredEntityWithReverseClustering.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
				.fromClusterings(4)
                .toClusterings(2)
                .orderByAscending()
                .get();

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
		CompoundPK compoundPK = new CompoundPK(partitionKey, count, name);
		ClusteredEntityWithReverseClustering entity = new ClusteredEntityWithReverseClustering(compoundPK,
				clusteredValue);
		manager.insert(entity);
	}
}
