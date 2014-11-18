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

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTypeTransformer;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTypeTransformer.ClusteredKey;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTypeTransformer.MyCount;
import static org.fest.assertions.api.Assertions.assertThat;

public class ClusteredEntityWithTypeTransformerIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, ClusteredEntityWithTypeTransformer.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_insert_and_find_entity_with_transformed_types() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        MyCount myCount = new MyCount(123);

        final ClusteredEntityWithTypeTransformer entity = new ClusteredEntityWithTypeTransformer(id, myCount, "val");

        manager.insert(entity);

        //When
        final ClusteredEntityWithTypeTransformer found = manager.find(ClusteredEntityWithTypeTransformer.class, new ClusteredKey(id,myCount));

        //Then
        assertThat(found.getId().getCount()).isEqualTo(myCount);
        assertThat(found.getValue()).isEqualTo("val");
    }
    
    @Test
    public void should_slice_query_with_transformed_types() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        MyCount myCount1 = new MyCount(1);
        MyCount myCount2 = new MyCount(2);
        MyCount myCount3 = new MyCount(3);

        final ClusteredEntityWithTypeTransformer entity1 = new ClusteredEntityWithTypeTransformer(id, myCount1, "val1");
        final ClusteredEntityWithTypeTransformer entity2 = new ClusteredEntityWithTypeTransformer(id, myCount2, "val2");
        final ClusteredEntityWithTypeTransformer entity3 = new ClusteredEntityWithTypeTransformer(id, myCount3, "val3");

        manager.insert(entity1);
        manager.insert(entity2);
        manager.insert(entity3);

        //When
        final List<ClusteredEntityWithTypeTransformer> found = manager.sliceQuery(ClusteredEntityWithTypeTransformer.class)
                .forSelect()
                .withPartitionComponents(id)
                .fromClusterings(myCount1)
                .get(2);

        //Then
        assertThat(found).hasSize(2);
        assertThat(found.get(0).getId().getCount()).isEqualTo(myCount1);
        assertThat(found.get(1).getId().getCount()).isEqualTo(myCount2);
    }

    @Test
    public void should_perform_typed_query() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        MyCount myCount1 = new MyCount(1);
        MyCount myCount2 = new MyCount(2);
        MyCount myCount3 = new MyCount(3);

        final ClusteredEntityWithTypeTransformer entity1 = new ClusteredEntityWithTypeTransformer(id, myCount1, "val1");
        final ClusteredEntityWithTypeTransformer entity2 = new ClusteredEntityWithTypeTransformer(id, myCount2, "val2");
        final ClusteredEntityWithTypeTransformer entity3 = new ClusteredEntityWithTypeTransformer(id, myCount3, "val3");

        manager.insert(entity1);
        manager.insert(entity2);
        manager.insert(entity3);

        //When
        final Select select = select().from(ClusteredEntityWithTypeTransformer.TABLE_NAME).where(eq("id", bindMarker("id"))).limit(bindMarker("lim"));
        final List<ClusteredEntityWithTypeTransformer> found = manager.typedQuery(ClusteredEntityWithTypeTransformer.class, select, id, 2).get();

        //Then
        assertThat(found).hasSize(2);
        assertThat(found.get(0).getId().getCount()).isEqualTo(myCount1);
        assertThat(found.get(1).getId().getCount()).isEqualTo(myCount2);
    }
}
