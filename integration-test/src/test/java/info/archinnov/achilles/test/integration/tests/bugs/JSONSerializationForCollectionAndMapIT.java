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

import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithJSONOnCollectionAndMap;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.fest.assertions.data.MapEntry;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.assertThat;

public class JSONSerializationForCollectionAndMapIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, EntityWithJSONOnCollectionAndMap.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_encode() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final EntityWithJSONOnCollectionAndMap entity = new EntityWithJSONOnCollectionAndMap(id, Arrays.asList(1,2), Sets.newHashSet(3), ImmutableMap.of(1,100), ImmutableMap.of(2,200), ImmutableMap.of(3,300));

        manager.insert(entity);

        //When
        final TypedMap found = manager.nativeQuery(select().from(EntityWithJSONOnCollectionAndMap.TABLE_NAME).where(eq("id", id))).getFirst();

        //Then
        assertThat(found.<List<String>>getTyped("mylist")).containsExactly("1","2");
        assertThat(found.<Set<String>>getTyped("myset")).containsExactly("3");
        assertThat(found.<Map<String, Integer>>getTyped("keymap")).contains(MapEntry.entry("1",100));
        assertThat(found.<Map<String, Integer>>getTyped("valuemap")).contains(MapEntry.entry(2,"200"));
        assertThat(found.<Map<String, Integer>>getTyped("keyvaluemap")).contains(MapEntry.entry("3","300"));
    }

    @Test
    public void should_decode() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final Insert insert = insertInto(EntityWithJSONOnCollectionAndMap.TABLE_NAME)
                .value("id", id)
                .value("mylist", Arrays.asList("1", "2"))
                .value("myset", Sets.newHashSet("3"))
                .value("keymap", ImmutableMap.of("1", 100))
                .value("valuemap", ImmutableMap.of(2, "200"))
                .value("keyvaluemap", ImmutableMap.of("3", "300"));

        manager.nativeQuery(insert).execute();

        //When
        final EntityWithJSONOnCollectionAndMap found = manager.find(EntityWithJSONOnCollectionAndMap.class,id);

        //Then
        assertThat(found.getMyList()).containsExactly(1,2);
        assertThat(found.getMySet()).containsExactly(3);
        assertThat(found.getKeyMap()).contains(MapEntry.entry(1,100));
        assertThat(found.getValueMap()).contains(MapEntry.entry(2,200));
        assertThat(found.getKeyValueMap()).contains(MapEntry.entry(3,300));
    }
}
