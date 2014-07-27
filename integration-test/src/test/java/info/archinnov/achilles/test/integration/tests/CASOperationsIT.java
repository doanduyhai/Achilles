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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static info.archinnov.achilles.listener.CASResultListener.CASResult;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation.INSERT;
import static info.archinnov.achilles.listener.CASResultListener.CASResult.Operation.UPDATE;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_SERIAL;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.Options.CASCondition;
import static info.archinnov.achilles.type.OptionsBuilder.casResultListener;
import static info.archinnov.achilles.type.OptionsBuilder.ifConditions;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.RegularStatement;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.exception.AchillesCASException;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.query.cql.NativeQuery;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.EntityWithEnum;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.OptionsBuilder;

public class CASOperationsIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(AchillesTestResource.Steps.AFTER_TEST, EntityWithEnum.TABLE_NAME, CompleteBean.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    @Test
    public void should_insert_when_not_exists() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);

        //When
        logAsserter.prepareLogLevel();
        manager.insert(entityWithEnum, OptionsBuilder.ifNotExists().casLocalSerial());
        final EntityWithEnum found = manager.find(EntityWithEnum.class, 10L);

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("name");
        assertThat(found.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
        logAsserter.assertSerialConsistencyLevels(LOCAL_SERIAL,ONE);
    }

    @Test
    public void should_insert_and_notify_cas_listener_on_success() throws Exception {
        final AtomicBoolean casSuccess = new AtomicBoolean(false);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
                casSuccess.compareAndSet(false, true);
            }

            @Override
            public void onCASError(CASResult casResult) {

            }
        };

        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);

        //When
        manager.insertOrUpdate(entityWithEnum, OptionsBuilder.ifNotExists().casResultListener(listener));
        final EntityWithEnum found = manager.find(EntityWithEnum.class, 10L);

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("name");
        assertThat(found.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
        assertThat(casSuccess.get()).isTrue();
    }

    @Test
    public void should_exception_when_trying_to_insert_with_cas_because_already_exist() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("id", 10L, "[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "name");
        AchillesCASException casException = null;
        manager.insert(entityWithEnum);

        //When
        try {
            manager.insert(entityWithEnum, OptionsBuilder.ifNotExists());
        } catch (AchillesCASException ace) {
            casException = ace;
        }

        assertThat(casException).isNotNull();
        assertThat(casException.operation()).isEqualTo(INSERT);
        assertThat(casException.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(casException.toString()).isEqualTo("CAS operation INSERT cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, id=10, name=name}");
    }

    @Test
    public void should_notify_listener_when_trying_to_insert_with_cas_because_already_exist() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("id", 10L, "[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "name");
        manager.insert(entityWithEnum);

        manager.insert(entityWithEnum, OptionsBuilder.ifNotExists().casResultListener(listener));

        final CASResult casResult = atomicCASResult.get();
        assertThat(casResult.operation()).isEqualTo(INSERT);
        assertThat(casResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(casResult.toString()).isEqualTo("CAS operation INSERT cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, id=10, name=name}");
    }


    @Test
    public void should_notify_listener_when_trying_to_insert_with_cas_and_ttl_because_already_exist() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("id", 10L, "[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "name");
        manager.insert(entityWithEnum);

        manager.insert(entityWithEnum, OptionsBuilder.ifNotExists()
                .withTtl(100).casResultListener(listener));

        final CASResult casResult = atomicCASResult.get();
        assertThat(casResult.operation()).isEqualTo(INSERT);
        assertThat(casResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(casResult.toString()).isEqualTo("CAS operation INSERT cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, id=10, name=name}");
    }


    @Test
    public void should_update_with_cas_conditions_using_enum() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        final EntityWithEnum managed = manager.insert(entityWithEnum);
        managed.setName("Helen");

        //When
        manager.insertOrUpdate(managed, ifConditions(new CASCondition("name", "John"), new CASCondition("consistency_level", EACH_QUORUM)));

        //Then
        final EntityWithEnum found = manager.find(EntityWithEnum.class, 10L);

        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Helen");
        assertThat(found.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_update_with_cas_conditions_using_cql3_column_name() throws Exception {
        //Given
        final Long primaryKey = RandomUtils.nextLong();
        final CompleteBean entity = new CompleteBean();
        entity.setId(primaryKey);
        entity.setAge(32L);
        entity.setName("John");

        final CompleteBean managed = manager.insert(entity);
        managed.setName("Helen");

        //When
        manager.update(managed, ifConditions(new CASCondition("age_in_years", 32L)));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, primaryKey);

        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Helen");
    }

    @Test
    public void should_exception_when_failing_cas_update() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        final EntityWithEnum managed = manager.insert(entityWithEnum);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "John");
        AchillesCASException casException = null;
        managed.setName("Helen");

        //When
        try {
            manager.update(managed, ifConditions(new CASCondition("name", "name"), new CASCondition("consistency_level", EACH_QUORUM)));
        } catch (AchillesCASException ace) {
            casException = ace;
        }

        assertThat(casException).isNotNull();
        assertThat(casException.operation()).isEqualTo(UPDATE);
        assertThat(casException.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(casException.toString()).isEqualTo("CAS operation UPDATE cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, name=John}");
    }

    @Test
    public void should_notify_listener_when_failing_cas_update() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };

        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        final EntityWithEnum managed = manager.insert(entityWithEnum);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "John");
        managed.setName("Helen");

        //When
        manager.update(managed,
                ifConditions(new CASCondition("name", "name"), new CASCondition("consistency_level", EACH_QUORUM))
                        .casResultListener(listener));

        final CASResult casResult = atomicCASResult.get();
        assertThat(casResult).isNotNull();
        assertThat(casResult.operation()).isEqualTo(UPDATE);
        assertThat(casResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(casResult.toString()).isEqualTo("CAS operation UPDATE cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, name=John}");
    }

    @Test
    public void should_notify_listener_when_failing_cas_update_with_ttl() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };

        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        final EntityWithEnum managed = manager.insert(entityWithEnum);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "John");
        managed.setName("Helen");

        //When
        manager.update(managed,
                ifConditions(new CASCondition("name", "name"), new CASCondition("consistency_level", EACH_QUORUM))
                        .casResultListener(listener)
                        .withTtl(100));

        final CASResult casResult = atomicCASResult.get();
        assertThat(casResult).isNotNull();
        assertThat(casResult.operation()).isEqualTo(UPDATE);
        assertThat(casResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(casResult.toString()).isEqualTo("CAS operation UPDATE cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, name=John}");
    }

    @Test
    public void should_update_set_with_cas_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").addFollowers("Paul", "Andrew").buid();
        final CompleteBean managed = manager.insert(entity);
        managed.getFollowers().add("Helen");
        managed.getFollowers().remove("Paul");

        //When
        manager.update(managed, ifConditions(new CASCondition("name", "John")).withTtl(100));

        //Then
        final CompleteBean actual = manager.find(CompleteBean.class, entity.getId());
        assertThat(actual.getFollowers()).containsOnly("Helen", "Andrew");
    }

    /**
     * Ignore until https://issues.apache.org/jira/browse/CASSANDRA-7499 is solved
     * @throws Exception
     */
    @Ignore
    @Test
    public void should_update_list_at_index_with_cas_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").addFriends("Paul", "Andrew").buid();
        final CompleteBean managed = manager.insert(entity);
        managed.getFriends().set(0, "Helen");
        managed.getFriends().set(1, null);

        //When
        manager.update(managed, ifConditions(new CASCondition("name", "John")).withTtl(100));

        //Then
        final CompleteBean actual = manager.find(CompleteBean.class, entity.getId());
        assertThat(actual.getFriends()).containsExactly("Helen");
    }

    @Test
    public void should_notify_listener_on_cas_update_failure() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "name", "John");

        CompleteBean entity = builder().randomId().name("John").addFollowers("Paul", "Andrew").buid();
        final CompleteBean managed = manager.insert(entity);
        managed.getFollowers().add("Helen");

        //When
        manager.update(managed, ifConditions(new CASCondition("name", "Helen")).casResultListener(listener));

        //Then
        final CASResult casResult = atomicCASResult.get();
        assertThat(casResult).isNotNull();
        assertThat(casResult.operation()).isEqualTo(UPDATE);
        assertThat(casResult.currentValues()).isEqualTo(expectedCurrentValues);

    }

    @Test
    public void should_notify_listener_when_cas_error_on_native_query() throws Exception {
        //Given
        final AtomicReference<CASResult> atomicCASResult = new AtomicReference(null);
        CASResultListener listener = new CASResultListener() {
            @Override
            public void onCASSuccess() {
            }

            @Override
            public void onCASError(CASResult casResult) {
                atomicCASResult.compareAndSet(null, casResult);
            }
        };
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "name", "John");

        CompleteBean entity = builder().randomId().name("John").buid();
        manager.insert(entity);

        final RegularStatement statement = update("CompleteBean").with(set("name","Helen"))
                .where(eq("id",entity.getId())).onlyIf(eq("name","Andrew"));

        //When
        final NativeQuery nativeQuery = manager.nativeQuery(statement,casResultListener(listener));
        nativeQuery.execute();

        //Then
        final CASResult casResult = atomicCASResult.get();

        assertThat(casResult).isNotNull();
        assertThat(casResult.operation()).isEqualTo(UPDATE);
        assertThat(casResult.currentValues()).isEqualTo(expectedCurrentValues);
    }
}
