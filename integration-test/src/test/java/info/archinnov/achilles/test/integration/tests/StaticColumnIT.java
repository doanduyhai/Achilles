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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticColumn.ClusteredKey;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticCounter.ClusteredKeyForCounter;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticColumn;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticCounter;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;
import net.sf.cglib.proxy.Factory;

public class StaticColumnIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
            ClusteredEntityWithStaticColumn.TABLE_NAME, ClusteredEntityWithStaticCounter.TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_query_static_column() throws Exception {
        Long partitionKey = RandomUtils.nextLong();
        ClusteredEntityWithStaticColumn parisStreet1 = new ClusteredEntityWithStaticColumn(new ClusteredKey(partitionKey,"street1"),"Paris","rue de la paix");
        ClusteredEntityWithStaticColumn parisStreet2 = new ClusteredEntityWithStaticColumn(new ClusteredKey(partitionKey,"street2"),"Paris","avenue des Champs Elysees");

        manager.persist(parisStreet1);
        manager.persist(parisStreet2);

        List<ClusteredEntityWithStaticColumn> found = manager.sliceQuery(ClusteredEntityWithStaticColumn.class)
                .partitionComponents(partitionKey)
                .get(100);

        assertThat(found).hasSize(2);
        final ClusteredEntityWithStaticColumn foundParisStreet1 = found.get(0);
        final ClusteredEntityWithStaticColumn foundParisStreet2 = found.get(1);

        assertThat(foundParisStreet1.getStreet()).isEqualTo("rue de la paix");
        assertThat(foundParisStreet2.getStreet()).isEqualTo("avenue des Champs Elysees");

        ClusteredEntityWithStaticColumn lyonStreet3 = new ClusteredEntityWithStaticColumn(new ClusteredKey(partitionKey,"street3"),"Lyon","rue Lamartine");
        manager.persist(lyonStreet3);

        found = manager.sliceQuery(ClusteredEntityWithStaticColumn.class)
                .partitionComponents(partitionKey)
                .get(100);

        assertThat(found).hasSize(3);
        final ClusteredEntityWithStaticColumn foundLyonStreet1 = found.get(0);
        final ClusteredEntityWithStaticColumn foundLyonStreet2 = found.get(1);
        final ClusteredEntityWithStaticColumn foundLyonStreet3 = found.get(2);

        assertThat(foundLyonStreet1.getStreet()).isEqualTo("rue de la paix");
        assertThat(foundLyonStreet1.getCity()).isEqualTo("Lyon");
        assertThat(foundLyonStreet2.getStreet()).isEqualTo("avenue des Champs Elysees");
        assertThat(foundLyonStreet3.getCity()).isEqualTo("Lyon");
        assertThat(foundLyonStreet3.getStreet()).isEqualTo("rue Lamartine");
        assertThat(foundLyonStreet3.getCity()).isEqualTo("Lyon");
    }

    @Test
    public void should_update_static_and_non_static_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong();
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredKey(partitionKey,"street1"),"Paris","rue de la paix");

        final ClusteredEntityWithStaticColumn managed = manager.persist(parisStreet);

        //When
        managed.setStreet("rue Lamartine");
        manager.update(managed);

        //Then
        final ClusteredEntityWithStaticColumn updated = manager.find(ClusteredEntityWithStaticColumn.class, parisStreet.getId());
        assertThat(updated.getCity()).isEqualTo("Paris");
        assertThat(updated.getStreet()).isEqualTo("rue Lamartine");

        //When
        updated.setCity("Lyon");
        manager.update(updated);

        final ClusteredEntityWithStaticColumn staticUpdated = manager.find(ClusteredEntityWithStaticColumn.class, parisStreet.getId());
        assertThat(staticUpdated.getCity()).isEqualTo("Lyon");
        assertThat(staticUpdated.getStreet()).isEqualTo("rue Lamartine");
    }

    @Test
    public void should_query_static_counter_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong();
        Counter version = CounterBuilder.incr();

        ClusteredEntityWithStaticCounter count1 = new ClusteredEntityWithStaticCounter(new ClusteredKeyForCounter(partitionKey,"count1"),version,CounterBuilder.incr(11));
        ClusteredEntityWithStaticCounter count2 = new ClusteredEntityWithStaticCounter(new ClusteredKeyForCounter(partitionKey,"count2"),null,CounterBuilder.incr(12));

        //When
        manager.persist(count1);
        manager.persist(count2);

        //Then
        List<ClusteredEntityWithStaticCounter> found = manager.sliceQuery(ClusteredEntityWithStaticCounter.class)
                .partitionComponents(partitionKey)
                .get(100);

        assertThat(found).hasSize(2);
        final ClusteredEntityWithStaticCounter foundCount1 = found.get(0);
        final ClusteredEntityWithStaticCounter foundCount2 = found.get(1);

        assertThat(foundCount1.getCount().get()).isEqualTo(11L);
        assertThat(foundCount2.getCount().get()).isEqualTo(12L);

        ClusteredEntityWithStaticCounter count3 = new ClusteredEntityWithStaticCounter(new ClusteredKeyForCounter(partitionKey,"count3"),version,CounterBuilder.incr(13));
        manager.persist(count3);

        found = manager.sliceQuery(ClusteredEntityWithStaticCounter.class)
                .partitionComponents(partitionKey)
                .get(100);

        assertThat(found).hasSize(3);
        final ClusteredEntityWithStaticCounter foundNewCount1 = found.get(0);
        final ClusteredEntityWithStaticCounter foundNewCount2 = found.get(1);
        final ClusteredEntityWithStaticCounter foundNewCount3 = found.get(2);

        assertThat(foundNewCount1.getCount().get()).isEqualTo(11L);
        assertThat(foundNewCount1.getVersion().get()).isEqualTo(2L);
        assertThat(foundNewCount2.getCount().get()).isEqualTo(12L);
        assertThat(foundNewCount2.getVersion().get()).isEqualTo(2L);
        assertThat(foundNewCount3.getCount().get()).isEqualTo(13L);
        assertThat(foundNewCount3.getVersion().get()).isEqualTo(2L);
    }

    @Test
    public void should_update_static_counter_and_non_static_counter_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong();
        Counter version = CounterBuilder.incr(1L);
        final Counter count = CounterBuilder.incr(11);
        ClusteredEntityWithStaticCounter entity = new ClusteredEntityWithStaticCounter(new ClusteredKeyForCounter(partitionKey,"count1"),version, count);

        //When
        final ClusteredEntityWithStaticCounter managed = manager.persist(entity);
        managed.getCount().incr(2L);
        manager.update(managed);


        //Then
        ClusteredEntityWithStaticCounter updated = manager.find(ClusteredEntityWithStaticCounter.class,entity.getId());
        assertThat(updated.getCount().get()).isEqualTo(13L);
        assertThat(updated.getVersion().get()).isEqualTo(1L);


        //When
        updated.getVersion().incr(2L);
        manager.update(updated);


        //Then
        ClusteredEntityWithStaticCounter staticUpdated = manager.find(ClusteredEntityWithStaticCounter.class,entity.getId());
        assertThat(staticUpdated.getCount().get()).isEqualTo(13L);
        assertThat(staticUpdated.getVersion().get()).isEqualTo(3L);
    }

    @Test
    public void should_remove_static_and_non_static_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong();
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredKey(partitionKey,"street1"),"Paris","rue de la paix");

        final ClusteredEntityWithStaticColumn managed = manager.persist(parisStreet);

        //When
        manager.remove(managed);

        //Then
        final ClusteredEntityWithStaticColumn found = manager.find(ClusteredEntityWithStaticColumn.class, parisStreet.getId());

        //When
        assertThat(found).isNull();
    }

    @Test
    public void should_remove_static_counter_and_non_static_counter_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong();
        Counter version = CounterBuilder.incr(1L);
        final Counter count = CounterBuilder.incr(11);
        ClusteredEntityWithStaticCounter entity = new ClusteredEntityWithStaticCounter(new ClusteredKeyForCounter(partitionKey,"count1"),version, count);
        final ClusteredEntityWithStaticCounter managed = manager.persist(entity);

        //When
        manager.remove(managed);

        Thread.sleep(1000);

        //Then
        ClusteredEntityWithStaticCounter found = manager.find(ClusteredEntityWithStaticCounter.class,entity.getId());
        assertThat(found).isNull();
    }
}
