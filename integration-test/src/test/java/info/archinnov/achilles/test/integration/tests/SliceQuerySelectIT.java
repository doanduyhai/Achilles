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
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompositeClusteredEntity;

public class SliceQuerySelectIT {

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
    public void should_query_with_default_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(2, "name2")
                .toClusterings(4, "name4")
                .get();

        assertThat(entities).isEmpty();

        insertClusteredValues(partitionKey, 1, "name1", 3);
        insertClusteredValues(partitionKey, 2, "name2", 2);
        insertClusteredValues(partitionKey, 3, "name3", 2);
        insertClusteredValues(partitionKey, 4, "name4", 4);

        entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(2, "name21")
                .toClusterings(4, "name41").get();

        assertThat(entities).hasSize(5);

        assertThat(entities.get(0).getValue()).isEqualTo("value21");
        assertThat(entities.get(0).getId().getCount()).isEqualTo(2);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name21");

        assertThat(entities.get(1).getValue()).isEqualTo("value22");
        assertThat(entities.get(1).getId().getCount()).isEqualTo(2);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name22");

        assertThat(entities.get(2).getValue()).isEqualTo("value31");
        assertThat(entities.get(2).getId().getCount()).isEqualTo(3);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name31");

        assertThat(entities.get(3).getValue()).isEqualTo("value32");
        assertThat(entities.get(3).getId().getCount()).isEqualTo(3);
        assertThat(entities.get(3).getId().getName()).isEqualTo("name32");

        assertThat(entities.get(4).getValue()).isEqualTo("value41");
        assertThat(entities.get(4).getId().getCount()).isEqualTo(4);
        assertThat(entities.get(4).getId().getName()).isEqualTo("name41");
    }

    @Test
    public void should_check_for_common_operation_on_found_clustered_entity() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertClusteredValues(partitionKey, 1, "name1", 1);

        ClusteredEntity clusteredEntity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByAscending()
                .getOne();

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
    public void should_query_with_custom_params() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertClusteredValues(partitionKey, 3, "name3", 2);
        insertClusteredValues(partitionKey, 4, "name4", 4);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(3, "name31")
                .toClusterings(4, "name41")
                .fromExclusiveToInclusiveBounds()
                .orderByDescending()
                .limit(2)
                .get();

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo("value41");
        assertThat(entities.get(1).getValue()).isEqualTo("value32");
    }

    @Test
    public void should_query_with_consistency_level() throws Exception {
        Long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 1, "name1", 5);

        exception.expect(InvalidQueryException.class);
        exception.expectMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");

        manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings(1, "name2")
                .toClusterings(1, "name4")
                .withConsistency(EACH_QUORUM)
                .get();
    }

    @Test
    public void should_get_one() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByAscending()
                .getOne();

        assertThat(entity).isNull();

        insertClusteredValues(partitionKey, 1, "name1", 5);

        entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .getOne();

        assertThat(entity.getValue()).isEqualTo("value11");

    }

    @Test
    public void should_get_by_descending() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertClusteredValues(partitionKey, 1, "name1", 5);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .get(3);

        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo("value15");
        assertThat(entities.get(1).getValue()).isEqualTo("value14");
        assertThat(entities.get(2).getValue()).isEqualTo("value13");

    }

    @Test
    public void should_get_first_matching() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        insertClusteredValues(partitionKey, 4, "name4", 2);

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .getFirstMatching(3, 4);

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue()).isEqualTo("value41");
        assertThat(entities.get(1).getValue()).isEqualTo("value42");
    }

    @Test
    public void should_get_last_matching() throws Exception {
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertClusteredValues(partitionKey, 1, "name1", 5);

        ClusteredEntity entity = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .getOne();

        assertThat(entity.getValue()).isEqualTo("value15");

        List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .orderByDescending()
                .get(3);

        assertThat(entities).hasSize(3);
        assertThat(entities.get(0).getValue()).isEqualTo("value15");
        assertThat(entities.get(1).getValue()).isEqualTo("value14");
        assertThat(entities.get(2).getValue()).isEqualTo("value13");

        insertClusteredValues(partitionKey, 4, "name4", 5);

        entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .getLastMatching(3, 4);

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue()).isEqualTo("value45");
        assertThat(entities.get(1).getValue()).isEqualTo("value44");
        assertThat(entities.get(2).getValue()).isEqualTo("value43");
    }


    @Test
    public void should_get_with_partition_keys_IN() throws Exception {
        long pk1 = RandomUtils.nextLong(0,Long.MAX_VALUE);
        long pk2 = pk1 + 1;
        long pk3 = pk1 + 2;
        long pk4 = pk1 + 3;

        insertClusteredValues(pk1, 1, "name", 1);
        insertClusteredValues(pk2, 1, "name", 1);
        insertClusteredValues(pk3, 1, "name", 2);
        insertClusteredValues(pk4, 1, "name", 1);

        final List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponentsIN(pk1, pk3)
                .get(5);

        Collections.sort(entities, new ClusteredEntity.ClusteredEntityComparator());
        entities.toString();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getId().getId()).isEqualTo(pk1);
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getId()).isEqualTo(pk3);
        assertThat(entities.get(1).getValue()).isEqualTo("value11");

        assertThat(entities.get(2).getId().getId()).isEqualTo(pk3);
        assertThat(entities.get(2).getValue()).isEqualTo("value12");
    }

    @Test
    public void should_get_with_partition_keys_IN_and_from_clusterings() throws Exception {
        long pk1 = RandomUtils.nextLong(0,Long.MAX_VALUE);
        long pk2 = pk1 + 1;
        long pk3 = pk1 + 2;
        long pk4 = pk1 + 3;

        insertClusteredValues(pk1, 1, "name1", 1);
        insertClusteredValues(pk2, 1, "abc", 1);
        insertClusteredValues(pk3, 1, "name1", 1);
        insertClusteredValues(pk4, 1, "abc", 1);

        final List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponentsIN(pk1, pk2, pk3, pk4)
                .fromClusterings(1, "name1")
                .get(5);

        Collections.sort(entities, new ClusteredEntity.ClusteredEntityComparator());

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getId().getId()).isEqualTo(pk1);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name11");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getId()).isEqualTo(pk3);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name11");
        assertThat(entities.get(1).getValue()).isEqualTo("value11");
    }

    @Test
    public void should_get_with_partition_keys_IN_and_with_clusterings_IN() throws Exception {
        long pk1 = RandomUtils.nextLong(0,Long.MAX_VALUE);
        long pk2 = pk1 + 1;
        long pk3 = pk1 + 2;
        long pk4 = pk1 + 3;

        insertClusteredValues(pk1, 1, "name1", 1);
        insertClusteredValues(pk2, 1, "nameX", 1);
        insertClusteredValues(pk3, 1, "name1", 1);
        insertClusteredValues(pk4, 1, "nameY", 2);

        final List<ClusteredEntity> entities = manager.sliceQuery(ClusteredEntity.class)
                .forSelect()
                .withPartitionComponentsIN(pk1, pk2, pk3, pk4)
                .withClusterings(1)
                .andClusteringsIN("nameX1", "nameY1", "nameY2")
                .get(5);

        Collections.sort(entities, new ClusteredEntity.ClusteredEntityComparator());

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getId().getId()).isEqualTo(pk2);
        assertThat(entities.get(0).getId().getName()).isEqualTo("nameX1");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getId()).isEqualTo(pk4);
        assertThat(entities.get(1).getId().getName()).isEqualTo("nameY1");
        assertThat(entities.get(1).getValue()).isEqualTo("value11");

        assertThat(entities.get(2).getId().getId()).isEqualTo(pk4);
        assertThat(entities.get(2).getId().getName()).isEqualTo("nameY2");
        assertThat(entities.get(2).getValue()).isEqualTo("value12");
    }

    /*
     *
     * SELECT FROM COMPOSITE CLUSTERED ENTITY
     *
     */
    @Test
    public void should_get_with_partition_keys_and_partition_keys_IN() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertCompositeClusteredValues(partitionKey,"bucket1",1,"name",1);
        insertCompositeClusteredValues(partitionKey,"bucket2",1,"name",1);
        insertCompositeClusteredValues(partitionKey,"bucket3",1,"name",1);

        //When
        final List<CompositeClusteredEntity> entities = manager.sliceQuery(CompositeClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1", "bucket3")
                .get(10);

        Collections.sort(entities, new CompositeClusteredEntity.CompositeClusteredEntityComparator());

        //Then
        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getId().getBucket()).isEqualTo("bucket1");
        assertThat(entities.get(0).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");

        assertThat(entities.get(1).getId().getBucket()).isEqualTo("bucket3");
        assertThat(entities.get(1).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(1).getValue()).isEqualTo("value11");
    }

    @Test
    public void should_get_with_partition_keys_and_partition_keys_IN_and_from_clusterings() throws Exception {
        //Given
        long partitionKey = RandomUtils.nextLong(0,Long.MAX_VALUE);

        insertCompositeClusteredValues(partitionKey,"bucket1",1,"abc",1);
        insertCompositeClusteredValues(partitionKey,"bucket2",1,"name",1);
        insertCompositeClusteredValues(partitionKey,"bucket3",1,"name",1);

        //When
        final List<CompositeClusteredEntity> entities = manager.sliceQuery(CompositeClusteredEntity.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .andPartitionComponentsIN("bucket1", "bucket3")
                .fromClusterings(1,"name1")
                .get(10);

        Collections.sort(entities, new CompositeClusteredEntity.CompositeClusteredEntityComparator());

        //Then
        assertThat(entities).hasSize(1);

        assertThat(entities.get(0).getId().getBucket()).isEqualTo("bucket3");
        assertThat(entities.get(0).getId().getName()).isEqualTo("name1");
        assertThat(entities.get(0).getValue()).isEqualTo("value11");
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
