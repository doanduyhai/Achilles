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

import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.options.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;

import com.datastax.driver.core.SimpleStatement;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.Batch;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithConsistencyLevelOnClassAndField;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

public class ConsistencyLevelPriorityOrderingIT {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

    private PersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    // Normal type
    @Test
    public void should_override_mapping_on_class_by_runtime_value_on_batch_mode_for_normal_type() throws Exception {
        EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
        long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        entity.setId(id);
        entity.setName("name");

        manager.insert(entity);

        Batch batch = pmf.createLoggedBatch();
        batch.startBatch(ONE);

        final EntityWithConsistencyLevelOnClassAndField proxy = manager.forUpdate(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());
        proxy.setName("changed_name");
        logAsserter.prepareLogLevelForDriverConnection();

        batch.update(proxy);

        batch.endBatch();
        logAsserter.assertConsistencyLevels(ONE);
        assertThatBatchContextHasBeenReset(batch);

        entity = manager.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId(), withConsistency(ONE));
        assertThat(entity.getName()).isEqualTo("changed_name");

        expectedEx.expect(NoHostAvailableException.class);
        expectedEx.expectMessage("Not enough replicas available for query at consistency THREE (3 required but only 1 alive)");
        manager.find(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());
    }

    // Counter type
    @Test
    public void should_override_mapping_on_class_by_mapping_on_field_for_counter_type() throws Exception {
        EntityWithConsistencyLevelOnClassAndField entity = new EntityWithConsistencyLevelOnClassAndField();
        entity.setId(RandomUtils.nextLong(0,Long.MAX_VALUE));
        entity.setName("name");
        entity.setCount(CounterBuilder.incr());

        manager.insert(entity);

        logAsserter.prepareLogLevelForDriverConnection();

        final EntityWithConsistencyLevelOnClassAndField proxy = manager.forUpdate(EntityWithConsistencyLevelOnClassAndField.class, entity.getId());

        Counter counter = proxy.getCount();
        counter.incr(10L);
        manager.update(proxy);
        logAsserter.assertConsistencyLevels(ONE);

        StringBuilder query = new StringBuilder("");
        query.append("SELECT ")
            .append(ACHILLES_COUNTER_VALUE)
            .append(" FROM ")
            .append(ACHILLES_COUNTER_TABLE)
            .append(" WHERE ")
            .append(ACHILLES_COUNTER_FQCN).append("='").append(EntityWithConsistencyLevelOnClassAndField.class.getCanonicalName()).append("'")
            .append(" AND ")
            .append(ACHILLES_COUNTER_PRIMARY_KEY).append("='").append(entity.getId()).append("'")
            .append(" AND ")
            .append(ACHILLES_COUNTER_PROPERTY_NAME).append("='count'");

        final TypedMap first = manager.nativeQuery(new SimpleStatement(query.toString())).getFirst();

        assertThat(first.<Long>get(ACHILLES_COUNTER_VALUE)).isEqualTo(11L);

    }

    private void assertThatBatchContextHasBeenReset(Batch batchEm) {
        BatchingFlushContext flushContext = Whitebox.getInternalState(batchEm, BatchingFlushContext.class);
        ConsistencyLevel consistencyLevel = Whitebox.getInternalState(flushContext, "consistencyLevel");
        List<AbstractStatementWrapper> statementWrappers = Whitebox.getInternalState(flushContext, "statementWrappers");

        assertThat(consistencyLevel).isEqualTo(ConsistencyLevel.ONE);
        assertThat(statementWrappers).isEmpty();
    }

}
