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

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithSecondaryIndex;
import info.archinnov.achilles.test.integration.entity.EntityWithSecondaryIndexOnEnum;
import info.archinnov.achilles.type.IndexCondition;

public class SecondaryIndexIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, CompleteBean.class.getSimpleName(), EntityWithSecondaryIndex.class.getSimpleName());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_return_entities_for_indexed_query() throws Exception {
        CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
        CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId().name("John DOO").buid();

        manager.insert(entity1);
        manager.insert(entity2);

        IndexCondition condition = new IndexCondition("name", "John DOO");
        List<CompleteBean> actual = manager.indexedQuery(CompleteBean.class, condition).get();

        assertThat(actual).hasSize(1);

        CompleteBean found1 = actual.get(0);
        assertThat(found1).isNotNull();

    }

    @Test
    public void should_find_entities_for_indexed_enum_query() throws Exception {
        //Given
        EntityWithSecondaryIndexOnEnum entity1 = new EntityWithSecondaryIndexOnEnum(10L, EACH_QUORUM);
        EntityWithSecondaryIndexOnEnum entity2 = new EntityWithSecondaryIndexOnEnum(11L, EACH_QUORUM);
        EntityWithSecondaryIndexOnEnum entity3 = new EntityWithSecondaryIndexOnEnum(13L, LOCAL_QUORUM);

        manager.insert(entity1);
        manager.insert(entity2);
        manager.insert(entity3);

        //When
        IndexCondition condition = new IndexCondition("consistencyLevel", EACH_QUORUM);
        final List<EntityWithSecondaryIndexOnEnum> actual = manager.indexedQuery(EntityWithSecondaryIndexOnEnum.class, condition).get();

        //Then
        assertThat(actual).hasSize(2);
        final EntityWithSecondaryIndexOnEnum found1 = actual.get(0);
        final EntityWithSecondaryIndexOnEnum found2 = actual.get(1);

        assertThat(found1.getId()).isEqualTo(10L);
        assertThat(found2.getId()).isEqualTo(11L);


    }

    @Test
    public void should_throw_clustered_exception_for_indexed_query() throws Exception {
        IndexCondition condition = new IndexCondition("name", "John DOO");

        exception.expect(AchillesException.class);
        exception.expectMessage("Index query is not supported for clustered entity");

        manager.indexedQuery(ClusteredEntity.class, condition).get();
    }

    @Test
    public void should_throw_empty_condition_exception_for_indexed_query() throws Exception {

        exception.expect(AchillesException.class);
        exception.expectMessage("Index condition should not be null");

        manager.indexedQuery(CompleteBean.class, null).get();
    }
}
