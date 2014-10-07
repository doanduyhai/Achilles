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
import java.util.Iterator;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompositeClusteredEntity;

public class SliceQueryIterateIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(AchillesTestResource.Steps.AFTER_TEST, TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    /*
     *
     * ITERATE FROM CLUSTERED ENTITY
     *
     */
    @Test
    public void should_iterate_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 5);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .iterator();

        assertThat(iter.hasNext()).isTrue();
        ClusteredEntity next = iter.next();
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name11");
        assertThat(next.getValue()).isEqualTo("value11");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name12");
        assertThat(next.getValue()).isEqualTo("value12");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name13");
        assertThat(next.getValue()).isEqualTo("value13");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name14");
        assertThat(next.getValue()).isEqualTo("value14");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getCount()).isEqualTo(1);
        assertThat(next.getId().getName()).isEqualTo("name15");
        assertThat(next.getValue()).isEqualTo("value15");

        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_check_for_common_operation_on_found_clustered_entity_by_iterator() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 1);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .iterator();

        iter.hasNext();
        ClusteredEntity clusteredEntity = iter.next();

        // Check for update
        clusteredEntity.setValue("dirty");
        manager.update(clusteredEntity);

        ClusteredEntity check = manager.find(ClusteredEntity.class, clusteredEntity.getId());
        assertThat(check.getValue()).isEqualTo("dirty");

        // Check for refresh
        check.setValue("dirty_again");
        manager.update(check);

        manager.refresh(clusteredEntity);
        assertThat(clusteredEntity.getValue()).isEqualTo("dirty_again");

        // Check for remove
        manager.remove(clusteredEntity);
        assertThat(manager.find(ClusteredEntity.class, clusteredEntity.getId())).isNull();
    }

    @Test
    public void should_iterate_with_custom_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 5);
        insertClusteredValues(partitionKey, 1, "name2", 5);

        Iterator<ClusteredEntity> iter = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name13")
                .toClusterings(1, "name21")
                .iterator(2);

        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo("value13");

        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo("value14");

        assertThat(iter.hasNext()).isTrue();
        assertThat(iter.next().getValue()).isEqualTo("value15");

        assertThat(iter.hasNext()).isTrue();
        final ClusteredEntity next = iter.next();
        assertThat(next.getId().getName()).isEqualTo("name21");
        assertThat(next.getValue()).isEqualTo("value11");

        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_over_clusterings_components() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 3);
        insertClusteredValues(partitionKey, 2, "name2", 2);
        insertClusteredValues(partitionKey, 3, "name3", 1);
        insertClusteredValues(partitionKey, 4, "name4", 1);

        //When
        final Iterator<ClusteredEntity> iterator = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1)
                .fromInclusiveToExclusiveBounds()
                .limit(6)
                .iterator(2);

        //Then
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("value11");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("value12");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("value13");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("value21");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("value22");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getValue()).isEqualTo("value31");

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_with_clustering_IN() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 3);
        insertClusteredValues(partitionKey, 1, "name2", 2);
        insertClusteredValues(partitionKey, 1, "name3", 1);
        insertClusteredValues(partitionKey, 1, "name4", 1);

        //When
        final Iterator<ClusteredEntity> iterator = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .withClusterings(1)
                .andClusteringsIN("name11","name12","name13","name41")
                .limit(100)
                .iterator(2);

        //Then
        assertThat(iterator.hasNext()).isTrue();
        ClusteredEntity next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name11");
        assertThat(next.getValue()).isEqualTo("value11");

        assertThat(iterator.hasNext()).isTrue();
        next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name12");
        assertThat(next.getValue()).isEqualTo("value12");

        assertThat(iterator.hasNext()).isTrue();
        next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name13");
        assertThat(next.getValue()).isEqualTo("value13");

        assertThat(iterator.hasNext()).isTrue();
        next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name41");
        assertThat(next.getValue()).isEqualTo("value11");

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_iterate_with_clustering_matching() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 3);
        insertClusteredValues(partitionKey, 2, "name2", 2);
        insertClusteredValues(partitionKey, 3, "name3", 1);
        insertClusteredValues(partitionKey, 4, "name4", 1);

        //When
        final Iterator<ClusteredEntity> iterator = manager.sliceQuery(ClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .iteratorWithMatching(1);

        //Then
        assertThat(iterator.hasNext()).isTrue();
        ClusteredEntity next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name13");
        assertThat(next.getValue()).isEqualTo("value13");

        assertThat(iterator.hasNext()).isTrue();
        next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name12");
        assertThat(next.getValue()).isEqualTo("value12");

        assertThat(iterator.hasNext()).isTrue();
        next = iterator.next();
        assertThat(next.getId().getName()).isEqualTo("name11");
        assertThat(next.getValue()).isEqualTo("value11");


        assertThat(iterator.hasNext()).isFalse();
    }

    /*
     *
     * ITERATE FROM COMPOSITE CLUSTERED ENTITY
     *
     */
    @Test
    public void should_iterate_with_partition_keys_and_partition_keys_IN() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertCompositeClusteredValues(partitionKey,"bucket1",1,"name1",1);
        insertCompositeClusteredValues(partitionKey,"bucket2",1,"name2",1);
        insertCompositeClusteredValues(partitionKey,"bucket3",1,"name3",1);

        //When
        final Iterator<CompositeClusteredEntity> iterator = manager.sliceQuery(CompositeClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1", "bucket3")
                .iterator();

        //Then
        assertThat(iterator.hasNext()).isTrue();
        CompositeClusteredEntity next = iterator.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getBucket()).isEqualTo("bucket1");
        assertThat(next.getId().getName()).isEqualTo("name11");
        assertThat(next.getValue()).isEqualTo("value11");

        assertThat(iterator.hasNext()).isTrue();
        next = iterator.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getBucket()).isEqualTo("bucket3");
        assertThat(next.getId().getName()).isEqualTo("name31");
        assertThat(next.getValue()).isEqualTo("value11");
    }

    @Test
    public void should_iterate_with_partition_keys_and_partition_keys_IN_and_from_clusterings() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertCompositeClusteredValues(partitionKey,"bucket1",1,"abc",1);
        insertCompositeClusteredValues(partitionKey,"bucket2",1,"name",1);
        insertCompositeClusteredValues(partitionKey,"bucket3",1,"name",1);

        //When
        final Iterator<CompositeClusteredEntity> iterator = manager.sliceQuery(CompositeClusteredEntity.class)
                .forIteration()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1", "bucket3")
                .fromClusterings(1,"name1")
                .iterator();

        //Then
        assertThat(iterator.hasNext()).isTrue();
        CompositeClusteredEntity next = iterator.next();
        assertThat(next.getId().getBucket()).isEqualTo("bucket3");
        assertThat(next.getId().getName()).isEqualTo("name1");
        assertThat(next.getValue()).isEqualTo("value11");
    }

    private void insertClusteredValues(long partitionKey, int countValue, String name, int size) {
        String clusteredValuePrefix = "value";

        for (int i = 1; i <= size; i++) {
            insertClusteredEntity(partitionKey, countValue, name + i, clusteredValuePrefix + countValue + i);
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
