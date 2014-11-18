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

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithTypeTransformer;
import info.archinnov.achilles.type.NamingStrategy;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.Arrays;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.assertThat;

public class EntityWithTypeTransformerIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, EntityWithTypeTransformer.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_insert_and_find_entity_with_transformed_types() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Long longValue = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final EntityWithTypeTransformer entity = new EntityWithTypeTransformer(id, longValue, Arrays.asList(longValue), Sets.newSet(longValue),
                ImmutableMap.of(longValue, "longValue"), ImmutableMap.of(1, longValue), ImmutableMap.of(longValue, NamingStrategy.CASE_SENSITIVE));

        manager.insert(entity);

        //When
        final EntityWithTypeTransformer found = manager.find(EntityWithTypeTransformer.class, id);

        //Then
        assertThat(found.getLongToString()).isEqualTo(longValue);
        assertThat(found.getMyList()).contains(longValue);
        assertThat(found.getMySet()).contains(longValue);
        assertThat(found.getKeyMap()).hasSize(1).containsKey(longValue).containsValue("longValue");
        assertThat(found.getValueMap()).hasSize(1).containsKey(1).containsValue(longValue);
        assertThat(found.getKeyValueMap()).hasSize(1).containsKey(longValue).containsValue(NamingStrategy.CASE_SENSITIVE);
    }

    @Test
    public void should_insert_and_find_entity_with_null_transformed_types() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        Long longValue = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final EntityWithTypeTransformer entity = new EntityWithTypeTransformer();
        entity.setId(id);
        entity.setLongToString(longValue);

        manager.insert(entity);

        //When
        final EntityWithTypeTransformer found = manager.find(EntityWithTypeTransformer.class, id);

        //Then
        assertThat(found.getLongToString()).isEqualTo(longValue);
        assertThat(found.getMyList()).isNull();
        assertThat(found.getMySet()).isNull();
        assertThat(found.getKeyMap()).isNull();
        assertThat(found.getValueMap()).isNull();
        assertThat(found.getKeyValueMap()).isNull();
    }
}
