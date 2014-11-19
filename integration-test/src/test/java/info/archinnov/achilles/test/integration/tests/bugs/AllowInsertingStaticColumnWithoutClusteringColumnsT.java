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

import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticColumn;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.fest.assertions.api.Assertions.assertThat;

public class AllowInsertingStaticColumnWithoutClusteringColumnsT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, ClusteredEntityWithStaticColumn.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_insert_only_static_columns() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final ClusteredEntityWithStaticColumn entity = new ClusteredEntityWithStaticColumn(id, null, "city",null);

        manager.insert(entity);

        //When
        final TypedMap found = manager.nativeQuery(select().from(ClusteredEntityWithStaticColumn.TABLE_NAME).where(eq("id",id))).first();

        //Then
        assertThat(found.getTyped("name")).isNull();
        assertThat(found.<String>getTyped("city")).isEqualTo("city");
    }
    

}
