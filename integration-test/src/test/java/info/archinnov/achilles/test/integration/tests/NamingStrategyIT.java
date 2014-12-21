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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithNamingStrategy;
import info.archinnov.achilles.test.integration.entity.EntityWithNamingStrategy;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class NamingStrategyIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean");

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PersistenceManager manager = resource.getPersistenceManager();

    private Session session = resource.getNativeSession();


    @Test
    public void should_apply_snake_case_naming() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        EntityWithNamingStrategy entity = new EntityWithNamingStrategy(id, "fn", "ln", "nick");

        //When
        manager.insert(entity);

        //Then
        final Row row = session.execute("SELECT * from achilles_test.snake_case_naming WHERE my_id = ?", id).one();
        assertThat(row).isNotNull();
        assertThat(row.getLong("my_id")).isEqualTo(id);
        assertThat(row.getString("fn")).isEqualTo("fn");
        assertThat(row.getString("last_name")).isEqualTo("ln");
        assertThat(row.getString("nickName")).isEqualTo("nick");

        //When
        final EntityWithNamingStrategy found = manager.find(EntityWithNamingStrategy.class, id);


        //Then
        assertThat(found.getFirstName()).isEqualTo("fn");
        assertThat(found.getLastName()).isEqualTo("ln");
        assertThat(found.getNickName()).isEqualTo("nick");
    }

    @Test
    public void should_apply_case_sensitive_naming() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final ClusteredEntityWithNamingStrategy first = new ClusteredEntityWithNamingStrategy(partitionKey, "1", "fn1", "ln1");
        final ClusteredEntityWithNamingStrategy second = new ClusteredEntityWithNamingStrategy(partitionKey, "2", "fn2", "ln2");

        manager.insert(first);
        manager.insert(second);

        //When
        final List<Row> rows = session.execute("SELECT * FROM achilles_test.\"caseSensitiveNaming\" WHERE \"partitionKey\" = ? LIMIT 10", partitionKey).all();

        //Then
        assertThat(rows).hasSize(2);
        final Row firstRow = rows.get(0);
        final Row secondRow = rows.get(1);

        assertThat(firstRow.getString("clustering")).isEqualTo("1");
        assertThat(firstRow.getString("firstName")).isEqualTo("fn1");
        assertThat(firstRow.getString("last_name")).isEqualTo("ln1");

        assertThat(secondRow.getString("clustering")).isEqualTo("2");
        assertThat(secondRow.getString("firstName")).isEqualTo("fn2");
        assertThat(secondRow.getString("last_name")).isEqualTo("ln2");
    }

    @Test
    public void should_perform_slice_query_with_naming_strategy() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final ClusteredEntityWithNamingStrategy first = new ClusteredEntityWithNamingStrategy(partitionKey, "1", "fn1", "ln1");
        final ClusteredEntityWithNamingStrategy second = new ClusteredEntityWithNamingStrategy(partitionKey, "2", "fn2", "ln2");
        final ClusteredEntityWithNamingStrategy third = new ClusteredEntityWithNamingStrategy(partitionKey, "3", "fn3", "ln3");
        final ClusteredEntityWithNamingStrategy fourth = new ClusteredEntityWithNamingStrategy(partitionKey, "4", "fn4", "ln4");

        manager.insert(first);
        manager.insert(second);
        manager.insert(third);
        manager.insert(fourth);

        //When
        final List<ClusteredEntityWithNamingStrategy> entities = manager.sliceQuery(ClusteredEntityWithNamingStrategy.class)
                .forSelect()
                .withPartitionComponents(partitionKey)
                .fromClusterings("2")
                .toClusterings("3")
                .get();

        //Then
        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getCompoundPK().getClusteringKey()).isEqualTo("2");
        assertThat(entities.get(0).getFirstName()).isEqualTo("fn2");
        assertThat(entities.get(0).getLastName()).isEqualTo("ln2");


        assertThat(entities.get(1).getCompoundPK().getClusteringKey()).isEqualTo("3");
        assertThat(entities.get(1).getFirstName()).isEqualTo("fn3");
        assertThat(entities.get(1).getLastName()).isEqualTo("ln3");
    }
}
