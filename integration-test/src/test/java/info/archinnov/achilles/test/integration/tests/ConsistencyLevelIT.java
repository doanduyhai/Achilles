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

import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_READ_MAP;
import static info.archinnov.achilles.configuration.ConfigurationParameters.CONSISTENCY_LEVEL_WRITE_MAP;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
import static info.archinnov.achilles.options.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Arrays;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.EntityWithTwoConsistency;
import info.archinnov.achilles.test.integration.entity.EntityWithWriteOneAndReadThreeConsistency;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;

public class ConsistencyLevelIT {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
            EntityWithTwoConsistency.TABLE_NAME, EntityWithWriteOneAndReadThreeConsistency.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    private Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);

    @Test
    public void should_throw_exception_when_persisting_with_two_consistency() throws Exception {
        EntityWithTwoConsistency bean = new EntityWithTwoConsistency();
        bean.setId(id);
        bean.setName("name");

        expectedEx.expect(UnavailableException.class);
        expectedEx.expectMessage("Not enough replica available for query at consistency TWO (2 required but only 1 alive)");

        manager.insert(bean);
    }

    @Test
    public void should_throw_exception_when_loading_entity_with_three_consistency() throws Exception {
        EntityWithWriteOneAndReadThreeConsistency bean = new EntityWithWriteOneAndReadThreeConsistency(id, "FN", "LN");

        manager.insert(bean);

        expectedEx.expect(UnavailableException.class);
        expectedEx.expectMessage("Not enough replica available for query at consistency THREE (3 required but only 1 alive)");

        manager.find(EntityWithWriteOneAndReadThreeConsistency.class, id);
    }

    @Test
    public void should_recover_from_exception_and_reinit_consistency_level() throws Exception {
        boolean exceptionCaught = false;
        EntityWithWriteOneAndReadThreeConsistency bean = new EntityWithWriteOneAndReadThreeConsistency(id, "FN", "LN");
        try {
            manager.insert(bean);
            manager.find(EntityWithWriteOneAndReadThreeConsistency.class, id);
        } catch (UnavailableException e) {
            // Should recover from exception
            exceptionCaught = true;
        }

        assertThat(exceptionCaught).isTrue();

        CompleteBean newBean = new CompleteBean();
        newBean.setId(id);
        newBean.setName("name");

        manager.insert(newBean);

        newBean = manager.find(CompleteBean.class, newBean.getId());

        assertThat(newBean).isNotNull();
        assertThat(newBean.getName()).isEqualTo("name");
    }

    @Test
    public void should_persist_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
        CompleteBean entity = builder().randomId().name("name zerferg").buid();

        logAsserter.prepareLogLevelForDriverConnection();
        manager.insert(entity, withConsistency(EACH_QUORUM));
        CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("name zerferg");
        logAsserter.assertConsistencyLevels(EACH_QUORUM, ONE);
    }

    @Test
    public void should_update_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
        CompleteBean entity = builder().randomId().name("name zeruioze").buid();
        manager.insert(entity);

        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.setName("zeruioze");

        logAsserter.prepareLogLevelForDriverConnection();
        manager.update(proxy, withConsistency(EACH_QUORUM));

        CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("zeruioze");
        logAsserter.assertConsistencyLevels(EACH_QUORUM, ONE);
    }

    @Test
    public void should_find_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
        boolean exceptionCaught = false;
        CompleteBean entity = builder().randomId().name("name rtprt").buid();
        manager.insert(entity);

        try {
            manager.find(CompleteBean.class, entity.getId(), withConsistency(EACH_QUORUM));
        } catch (InvalidQueryException e) {
            assertThat(e).hasMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");
            exceptionCaught = true;
        }

        assertThat(exceptionCaught).isTrue();

        logAsserter.prepareLogLevelForDriverConnection();
        CompleteBean found = manager.find(CompleteBean.class, entity.getId(), withConsistency(ALL));
        assertThat(found.getName()).isEqualTo("name rtprt");
        logAsserter.assertConsistencyLevels(ALL);
    }


    @Test
    public void should_delete_with_runtime_consistency_level_overriding_predefined_one() throws Exception {
        CompleteBean entity = builder().randomId().name("name").buid();
        manager.insert(entity);

        logAsserter.prepareLogLevelForDriverConnection();
        manager.delete(entity, withConsistency(EACH_QUORUM));
        assertThat(manager.find(CompleteBean.class, entity.getId())).isNull();
        logAsserter.assertConsistencyLevels(EACH_QUORUM,ONE);
    }

    @Test
    public void should_reinit_consistency_level_after_exception() throws Exception {
        CompleteBean entity = builder().randomId().name("name qzerferf").buid();
        logAsserter.prepareLogLevelForDriverConnection();
        manager.insert(entity, withConsistency(EACH_QUORUM));
        CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("name qzerferf");
        logAsserter.assertConsistencyLevels(EACH_QUORUM, ONE);
    }

    @Test
    public void should_override_consistency_by_map() throws Exception {
        //Given
        final PersistenceManager pm = CassandraEmbeddedServerBuilder
                .withEntities(Arrays.<Class<?>>asList(EntityWithTwoConsistency.class))
                .withKeyspaceName("consistency_map_override")
                .cleanDataFilesAtStartup(true)
                .withAchillesConfigParams(ImmutableMap.<ConfigurationParameters, Object>of(
                        CONSISTENCY_LEVEL_READ_MAP, ImmutableMap.of(EntityWithTwoConsistency.TABLE_NAME, QUORUM),
                        CONSISTENCY_LEVEL_WRITE_MAP, ImmutableMap.of(EntityWithTwoConsistency.TABLE_NAME, ONE)
                ))
                .buildPersistenceManager();

        try {
            //When
            logAsserter.prepareLogLevelForDriverConnection();
            final EntityWithTwoConsistency entity = new EntityWithTwoConsistency();
            entity.setId(RandomUtils.nextLong(0,Long.MAX_VALUE));

            pm.insert(entity);
            //Then
            assertThat(pm.find(EntityWithTwoConsistency.class, entity.getId())).isNotNull();
            logAsserter.assertConsistencyLevels(ONE, QUORUM);
        } finally {
            pm.getNativeSession().close();
        }


    }

}
