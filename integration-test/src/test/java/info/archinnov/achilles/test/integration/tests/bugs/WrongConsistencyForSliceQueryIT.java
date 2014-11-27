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

package info.archinnov.achilles.test.integration.tests.bugs;

import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithConsistencyLevel;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class WrongConsistencyForSliceQueryIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.BOTH, ClusteredEntityWithConsistencyLevel.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    @Test
    public void should_slice_query_with_class_consistency_level() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Date date1 = new Date(1);
        Date date2 = new Date(2);
        Date date3 = new Date(3);

        final ClusteredEntityWithConsistencyLevel entity1 = new ClusteredEntityWithConsistencyLevel(id, date1, "1");
        final ClusteredEntityWithConsistencyLevel entity2 = new ClusteredEntityWithConsistencyLevel(id, date2, "2");
        final ClusteredEntityWithConsistencyLevel entity3 = new ClusteredEntityWithConsistencyLevel(id, date3, "3");

        manager.insert(entity1);
        manager.insert(entity2);
        manager.insert(entity3);

        //When
        logAsserter.prepareLogLevel();

        final ClusteredEntityWithConsistencyLevel found = manager.sliceQuery(ClusteredEntityWithConsistencyLevel.class)
                .forSelect()
                .withPartitionComponents(id)
                .fromClusterings(date1)
                .getOne();

        //Then
        assertThat(found.getId().getDate()).isEqualTo(date1);
        logAsserter.assertConsistencyLevels(ConsistencyLevel.LOCAL_ONE);
    }

    @Test
    public void should_slice_query_with_runtime_consistency_level() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Date date1 = new Date(1);

        final ClusteredEntityWithConsistencyLevel entity1 = new ClusteredEntityWithConsistencyLevel(id, date1, "1");

        manager.insert(entity1);

        //When
        logAsserter.prepareLogLevel();

        final ClusteredEntityWithConsistencyLevel found = manager.sliceQuery(ClusteredEntityWithConsistencyLevel.class)
                .forSelect()
                .withPartitionComponents(id)
                .fromClusterings(date1)
                .withConsistency(ConsistencyLevel.ALL)
                .getOne();

        //Then
        assertThat(found.getId().getDate()).isEqualTo(date1);
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL);

    }
    
    @Test
    public void should_delete_with_custom_consistency_level() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Date date1 = new Date(1);
        Date date2 = new Date(2);
        Date date3 = new Date(3);

        final ClusteredEntityWithConsistencyLevel entity1 = new ClusteredEntityWithConsistencyLevel(id, date1, "1");
        final ClusteredEntityWithConsistencyLevel entity2 = new ClusteredEntityWithConsistencyLevel(id, date2, "2");
        final ClusteredEntityWithConsistencyLevel entity3 = new ClusteredEntityWithConsistencyLevel(id, date3, "3");

        manager.insert(entity1);
        manager.insert(entity2);
        manager.insert(entity3);

        //When
        logAsserter.prepareLogLevel();

        manager.sliceQuery(ClusteredEntityWithConsistencyLevel.class)
                .forDelete()
                .withPartitionComponents(id)
                .withConsistency(ConsistencyLevel.LOCAL_ONE)
                .deleteMatching(date1);

        logAsserter.assertConsistencyLevels(ConsistencyLevel.LOCAL_ONE);

        final List<ClusteredEntityWithConsistencyLevel> found = manager.sliceQuery(ClusteredEntityWithConsistencyLevel.class)
                .forSelect()
                .withPartitionComponents(id)
                .get(10);

        //Then
        assertThat(found).hasSize(2);
        assertThat(found.get(0).getValue()).isEqualTo("2");
        assertThat(found.get(1).getValue()).isEqualTo("3");
    }


}
