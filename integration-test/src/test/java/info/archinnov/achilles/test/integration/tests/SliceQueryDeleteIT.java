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
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompositeClusteredEntity;

public class SliceQueryDeleteIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(AchillesTestResource.Steps.AFTER_TEST, TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();


    /*
     *
     * SELECT FROM CLUSTERED ENTITY
     *
     */

    @Test
    public void should_delete_with_partition_keys() throws Exception {
        long pk1 = RandomUtils.nextLong();
        long pk2 = RandomUtils.nextLong();

        insertClusteredValues(pk1, 1, "name1", 1);
        insertClusteredValues(pk2, 1, "name21", 1);
        insertClusteredValues(pk2, 1, "name22", 1);

        manager.sliceQuery(ClusteredEntity.class)
                .forDelete()
                .withPartitionComponents(pk1)
                .delete();

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponentsIN(pk1, pk2)
                .get(100);

        assertThat(entities).hasSize(2);

        Collections.sort(entities, new ClusteredEntity.ClusteredEntityComparator());

        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name21");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name22");
        assertThat(entities.get(1).getValue()).isEqualTo("value11");
    }

    @Test
    public void should_delete_with_matching_clusterings() throws Exception {
        long partitionKey = RandomUtils.nextLong();

        insertClusteredValues(partitionKey, 1, "name1", 1);
        insertClusteredValues(partitionKey, 1, "name2", 3);
        insertClusteredValues(partitionKey, 1, "name3", 1);

        manager.sliceQuery(ClusteredEntity.class)
                .forDelete()
                .withPartitionComponents(partitionKey)
                .deleteMatching(1, "name2");

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .get(100);

        assertThat(entities).hasSize(2);

        Collections.sort(entities, new ClusteredEntity.ClusteredEntityComparator());

        assertThat(entities.get(0).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getCount()).isEqualTo(1);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(1).getValue()).isEqualTo("value11");
    }


    /*
     *
     * SELECT FROM COMPOSITE CLUSTERED ENTITY
     *
     */
    @Test
    public void should_delete_with_partition_keys_and_partition_keys_IN() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong();

        insertCompositeClusteredValues(partitionKey, "bucket1", 1, "name", 1);
        insertCompositeClusteredValues(partitionKey, "bucket2", 1, "name", 1);
        insertCompositeClusteredValues(partitionKey, "bucket3", 1, "name", 1);

        //When
        manager.sliceQuery(CompositeClusteredEntity.class)
                .forDelete()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1", "bucket3")
                .delete();

        final List<CompositeClusteredEntity> entities = manager.sliceQuery(CompositeClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey, "bucket2")
                .get(100);

        //Then
        assertThat(entities).hasSize(1);

        assertThat(entities.get(0).getId().getBucket()).isEqualTo("bucket2");
        assertThat(entities.get(0).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");
    }

    @Test
    public void should_delete_with_partition_keys_and_partition_keys_IN_and_from_clusterings() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong();

        insertCompositeClusteredValues(partitionKey, "bucket1", 1, "abc", 1);
        insertCompositeClusteredValues(partitionKey, "bucket2", 1, "name", 1);
        insertCompositeClusteredValues(partitionKey, "bucket3", 1, "name", 1);

        //When
        manager.sliceQuery(CompositeClusteredEntity.class)
                .forDelete()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1", "bucket3")
                .deleteMatching(1, "abc1");

        final List<CompositeClusteredEntity> entities = manager.sliceQuery(CompositeClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1","bucket2","bucket3")
                .get(100);

        //Then
        assertThat(entities).hasSize(2);

        Collections.sort(entities, new CompositeClusteredEntity.CompositeClusteredEntityComparator());

        assertThat(entities.get(0).getId().getBucket()).isEqualTo("bucket2");
        assertThat(entities.get(0).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getBucket()).isEqualTo("bucket3");
        assertThat(entities.get(1).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(1).getValue()).isEqualTo("value11");
    }


    private void insertClusteredValues(long partitionKey, int countValue, String name, int size) {
        String clusteredValuePrefix = "value";

        for (int i = 1; i <= size; i++) {
            insertClusteredEntity(partitionKey, countValue, name, clusteredValuePrefix + countValue + i);
        }
    }

    private void insertClusteredEntity(Long partitionKey, int count, String name, String clusteredValue) {
        ClusteredEntity.ClusteredKey embeddedId = new ClusteredEntity.ClusteredKey(partitionKey, count, name);
        ClusteredEntity entity = new ClusteredEntity(embeddedId, clusteredValue);
        manager.insert(entity);
    }

    private void insertCompositeClusteredValues(long id, String bucket, int countValue, String name, int size) {
        String clusteredValuePrefix = "value";
        for (int i = 1; i <= size; i++) {
            insertCompositeClusteredEntity(id, bucket, countValue, name + i, clusteredValuePrefix + countValue + i);
        }
    }

    private void insertCompositeClusteredEntity(long id, String bucket, int count, String name, String clusteredValue) {
        CompositeClusteredEntity.ClusteredKey embeddedId = new CompositeClusteredEntity.ClusteredKey(id, bucket, count, name);
        CompositeClusteredEntity entity = new CompositeClusteredEntity(embeddedId, clusteredValue);
        manager.insert(entity);
    }
}
