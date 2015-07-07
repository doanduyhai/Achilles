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
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation.INSERT;
import static info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation.UPDATE;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_SERIAL;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.options.OptionsBuilder.*;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.listener.LWTResultListener;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.RegularStatement;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.query.cql.NativeQuery;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.EntityWithEnum;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.options.OptionsBuilder;

public class LWTOperationsIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.BOTH, EntityWithEnum.TABLE_NAME, CompleteBean.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    @Test
    public void should_insert_when_not_exists() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);

        //When
        logAsserter.prepareLogLevelForDriverConnection();
        manager.insert(entityWithEnum, OptionsBuilder.ifNotExists().lwtLocalSerial());
        final EntityWithEnum found = manager.find(EntityWithEnum.class, 10L);

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("name");
        assertThat(found.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
        logAsserter.assertSerialConsistencyLevels(LOCAL_SERIAL,ONE);
    }

    @Test
    public void should_insert_and_notify_LWT_listener_on_success() throws Exception {
        final AtomicBoolean LWTSuccess = new AtomicBoolean(false);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
                LWTSuccess.compareAndSet(false, true);
            }

            @Override
            public void onError(LWTResult lwtResult) {

            }
        };

        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);

        //When
        manager.insertOrUpdate(entityWithEnum, OptionsBuilder.ifNotExists().lwtResultListener(listener));
        final EntityWithEnum found = manager.find(EntityWithEnum.class, 10L);

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("name");
        assertThat(found.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
        assertThat(LWTSuccess.get()).isTrue();
    }

    @Test
    public void should_exception_when_trying_to_insert_with_LWT_because_already_exist() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("id", 10L, "[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "name");
        AchillesLightWeightTransactionException lwtException = null;
        manager.insert(entityWithEnum);

        //When
        try {
            manager.insert(entityWithEnum, OptionsBuilder.ifNotExists());
        } catch (AchillesLightWeightTransactionException ace) {
            lwtException = ace;
        }

        assertThat(lwtException).isNotNull();
        assertThat(lwtException.operation()).isEqualTo(INSERT);
        assertThat(lwtException.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(lwtException.toString()).isEqualTo("CAS operation INSERT cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, id=10, name=name}");
    }

    @Test
    public void should_notify_listener_when_trying_to_insert_with_LWT_because_already_exist() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicLWTResult = new AtomicReference(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicLWTResult.compareAndSet(null, lwtResult);
            }
        };
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("id", 10L, "[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "name");
        manager.insert(entityWithEnum);

        manager.insert(entityWithEnum, OptionsBuilder.ifNotExists().lwtResultListener(listener));

        final LWTResultListener.LWTResult lwtResult = atomicLWTResult.get();
        assertThat(lwtResult.operation()).isEqualTo(INSERT);
        assertThat(lwtResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(lwtResult.toString()).isEqualTo("CAS operation INSERT cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, id=10, name=name}");
    }


    @Test
    public void should_notify_listener_when_trying_to_insert_with_lwt_and_ttl_because_already_exist() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicLWTResult = new AtomicReference(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicLWTResult.compareAndSet(null, lwtResult);
            }
        };
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "name", EACH_QUORUM);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("id", 10L, "[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "name");
        manager.insert(entityWithEnum);

        manager.insert(entityWithEnum, OptionsBuilder.ifNotExists()
                .withTtl(100).lwtResultListener(listener));

        final LWTResultListener.LWTResult LWTResult = atomicLWTResult.get();
        assertThat(LWTResult.operation()).isEqualTo(INSERT);
        assertThat(LWTResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(LWTResult.toString()).isEqualTo("CAS operation INSERT cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, id=10, name=name}");
    }


    @Test
    public void should_update_with_cas_conditions_using_enum() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        manager.insert(entityWithEnum);

        EntityWithEnum proxy = manager.forUpdate(EntityWithEnum.class, entityWithEnum.getId());
        proxy.setName("Helen");

        //When
        manager.insertOrUpdate(proxy, ifEqualCondition("name", "John").ifEqualCondition("consistency_level", EACH_QUORUM));

        //Then
        final EntityWithEnum found = manager.find(EntityWithEnum.class, 10L);

        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Helen");
        assertThat(found.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_update_with_cas_conditions_using_cql_column_name() throws Exception {
        //Given
        final Long primaryKey = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final CompleteBean entity = new CompleteBean();
        entity.setId(primaryKey);
        entity.setAge(32L);
        entity.setName("John");
        List<String> friends = new ArrayList<>();
        friends.add("Paul");
        entity.setFriends(friends);

        manager.insert(entity);

        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, primaryKey);
        proxy.setName("Helen");
        proxy.getFriends().add("George");

        //When
        manager.update(proxy, ifEqualCondition("age_in_years", 32L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, primaryKey);

        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("Helen");
    }

    @Test
    public void should_exception_when_failing_cas_update() throws Exception {
        //Given
        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        manager.insert(entityWithEnum);

        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "John");
        AchillesLightWeightTransactionException lwtException = null;


        final EntityWithEnum proxy = manager.forUpdate(EntityWithEnum.class, entityWithEnum.getId());

        proxy.setName("Helen");

        //When
        try {
            manager.update(proxy, ifEqualCondition("name", "name").ifEqualCondition("consistency_level", EACH_QUORUM));
        } catch (AchillesLightWeightTransactionException ace) {
            lwtException = ace;
        }

        assertThat(lwtException).isNotNull();
        assertThat(lwtException.operation()).isEqualTo(UPDATE);
        assertThat(lwtException.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(lwtException.toString()).isEqualTo("CAS operation UPDATE cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, name=John}");
    }

    @Test
    public void should_notify_listener_when_failing_cas_update() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicCASResult = new AtomicReference(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicCASResult.compareAndSet(null, lwtResult);
            }
        };

        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        manager.insert(entityWithEnum);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "John");

        final EntityWithEnum proxy = manager.forUpdate(EntityWithEnum.class, entityWithEnum.getId());

        proxy.setName("Helen");

        //When
        manager.update(proxy,ifEqualCondition("name", "name").ifEqualCondition("consistency_level", EACH_QUORUM).lwtResultListener(listener));

        final LWTResultListener.LWTResult LWTResult = atomicCASResult.get();
        assertThat(LWTResult).isNotNull();
        assertThat(LWTResult.operation()).isEqualTo(UPDATE);
        assertThat(LWTResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(LWTResult.toString()).isEqualTo("CAS operation UPDATE cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, name=John}");
    }

    @Test
    public void should_notify_listener_when_failing_cas_update_with_ttl() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicCASResult = new AtomicReference(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicCASResult.compareAndSet(null, lwtResult);
            }
        };

        final EntityWithEnum entityWithEnum = new EntityWithEnum(10L, "John", EACH_QUORUM);
        manager.insert(entityWithEnum);
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "consistency_level", EACH_QUORUM.name(), "name", "John");

        final EntityWithEnum proxy = manager.forUpdate(EntityWithEnum.class, entityWithEnum.getId());

        proxy.setName("Helen");

        //When
        manager.update(proxy,ifEqualCondition("name", "name")
                        .ifEqualCondition("consistency_level", EACH_QUORUM)
                        .lwtResultListener(listener)
                        .withTtl(100));

        final LWTResultListener.LWTResult LWTResult = atomicCASResult.get();
        assertThat(LWTResult).isNotNull();
        assertThat(LWTResult.operation()).isEqualTo(UPDATE);
        assertThat(LWTResult.currentValues()).isEqualTo(expectedCurrentValues);
        assertThat(LWTResult.toString()).isEqualTo("CAS operation UPDATE cannot be applied. Current values are: {[applied]=false, consistency_level=EACH_QUORUM, name=John}");
    }

    @Test
    public void should_update_set_with_cas_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").addFollowers("Paul", "Andrew").buid();
        manager.insert(entity);

        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.getFollowers().add("Helen");
        proxy.getFollowers().remove("Paul");

        //When
        manager.update(proxy, ifEqualCondition("name", "John").withTtl(100));

        //Then
        final CompleteBean actual = manager.find(CompleteBean.class, entity.getId());
        assertThat(actual.getFollowers()).containsOnly("Helen", "Andrew");
    }

    @Test
    public void should_update_list_at_index_with_cas_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").addFriends("Paul", "Andrew").buid();
        manager.insert(entity);

        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.getFriends().set(0, "Helen");
        proxy.getFriends().set(1, null);

        //When
        manager.update(proxy, ifEqualCondition("name", "John").withTtl(100));

        //Then
        final CompleteBean actual = manager.find(CompleteBean.class, entity.getId());
        assertThat(actual.getFriends()).containsExactly("Helen");
    }

    @Test
    public void should_notify_listener_on_LWT_update_failure() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicLWTResult = new AtomicReference(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicLWTResult.compareAndSet(null, lwtResult);
            }
        };
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "name", "John");

        CompleteBean entity = builder().randomId().name("John").addFollowers("Paul", "Andrew").buid();
        manager.insert(entity);

        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());

        proxy.getFollowers().add("Helen");

        //When
        manager.update(proxy, ifEqualCondition("name", "Helen").lwtResultListener(listener));

        //Then
        final LWTResultListener.LWTResult LWTResult = atomicLWTResult.get();
        assertThat(LWTResult).isNotNull();
        assertThat(LWTResult.operation()).isEqualTo(UPDATE);
        assertThat(LWTResult.currentValues()).isEqualTo(expectedCurrentValues);

    }

    @Test
    public void should_notify_listener_when_LWT_error_on_native_query() throws Exception {
        //Given
        final AtomicReference<LWTResultListener.LWTResult> atomicLWTResult = new AtomicReference(null);
        LWTResultListener listener = new LWTResultListener() {
            @Override
            public void onSuccess() {
            }

            @Override
            public void onError(LWTResult lwtResult) {
                atomicLWTResult.compareAndSet(null, lwtResult);
            }
        };
        Map<String, Object> expectedCurrentValues = ImmutableMap.<String, Object>of("[applied]", false, "name", "John");

        CompleteBean entity = builder().randomId().name("John").buid();
        manager.insert(entity);

        final RegularStatement statement = update("CompleteBean").with(set("name","Helen"))
                .where(eq("id",entity.getId())).onlyIf(eq("name", "Andrew"));

        //When
        final NativeQuery nativeQuery = manager.nativeQuery(statement, lwtResultListener(listener));
        nativeQuery.execute();

        //Then
        final LWTResultListener.LWTResult LWTResult = atomicLWTResult.get();

        assertThat(LWTResult).isNotNull();
        assertThat(LWTResult.operation()).isEqualTo(UPDATE);
        assertThat(LWTResult.currentValues()).isEqualTo(expectedCurrentValues);
    }

    @Test
    public void should_delete_if_exists() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").buid();
        manager.insert(entity);

        //When
        manager.deleteById(CompleteBean.class, entity.getId(), ifExists());
    }

    @Test(expected = AchillesLightWeightTransactionException.class)
    public void should_exception_when_deleting_non_existing() throws Exception {
        manager.deleteById(CompleteBean.class, 10L, ifExists());
    }

    @Test
    public void should_delete_with_lwt_conditions() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").age(33L).buid();
        manager.insert(entity);

        //When
        manager.deleteById(CompleteBean.class, entity.getId(),
                ifEqualCondition("name", "John").ifEqualCondition("age_in_years",33L));

        //Then
        assertThat(manager.find(CompleteBean.class, entity.getId())).isNull();
    }

    @Test
    public void should_update_with_neq_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").age(33L).buid();
        manager.insert(entity);

        //When
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());
        proxy.setName("John33");
        manager.update(proxy, OptionsBuilder.ifNotEqualCondition("age_in_years", 32L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("John33");
    }

    @Test
    public void should_update_with_gt_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").age(33L).buid();
        manager.insert(entity);

        //When
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());
        proxy.setName("John33");
        manager.update(proxy, OptionsBuilder.ifGreaterCondition("age_in_years", 32L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("John33");
    }

    @Test
    public void should_update_with_gte_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").age(33L).buid();
        manager.insert(entity);

        //When
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());
        proxy.setName("John33");
        manager.update(proxy,OptionsBuilder.ifGreaterOrEqualCondition("age_in_years", 33L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("John33");
    }

    @Test
    public void should_update_with_lt_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").age(33L).buid();
        manager.insert(entity);

        //When
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());
        proxy.setName("John33");
        manager.update(proxy,OptionsBuilder.ifLesserCondition("age_in_years",34L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("John33");
    }

    @Test
    public void should_update_with_lte_condition() throws Exception {
        //Given
        CompleteBean entity = builder().randomId().name("John").age(33L).buid();
        manager.insert(entity);

        //When
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, entity.getId());
        proxy.setName("John33");
        manager.update(proxy,OptionsBuilder.ifLesserOrEqualCondition("age_in_years",33L));

        //Then
        final CompleteBean found = manager.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("John33");
    }

    @Test(expected = AchillesLightWeightTransactionException.class)
    public void should_exception_after_update_if_exists() throws Exception {
        //Given
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, RandomUtils.nextLong(0,Long.MAX_VALUE));
        proxy.setName("test");

        //When
        manager.update(proxy,ifExists());
        //Then
    }

    @Test(expected = AchillesLightWeightTransactionException.class)
    public void should_exception_after_update_if_exists_for_set() throws Exception {
        //Given
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, RandomUtils.nextLong(0,Long.MAX_VALUE));
        proxy.getFollowers().add("John");

        //When
        manager.update(proxy,ifExists());
        //Then
    }

    @Test(expected = AchillesLightWeightTransactionException.class)
    public void should_exception_after_update_if_exists_for_list() throws Exception {
        //Given
        final CompleteBean proxy = manager.forUpdate(CompleteBean.class, RandomUtils.nextLong(0,Long.MAX_VALUE));
        proxy.getFriends().add(0,"John");

        //When
        manager.update(proxy,ifExists());
        //Then
    }
}
