/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.it;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_10;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_10;
import info.archinnov.achilles.generated.manager.EntityWithCaseSensitiveUdt_Manager;
import info.archinnov.achilles.internals.entities.EntityWithCaseSensitiveUdt;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

public class TestUdtWithCaseSensitiveUDTIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_10> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_10")
            .withScript("scripts/create_case_sensitive_objects.cql")
            .entityClassesToTruncate(EntityWithCaseSensitiveUdt.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_10
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithCaseSensitiveUdt.class)
                    .doForceSchemaCreation(false)
                    .withStatementsCache(statementsCache)
                    .build());

    private EntityWithCaseSensitiveUdt_Manager manager = resource.getManagerFactory().forEntityWithCaseSensitiveUdt();
    
    @Test
    public void should_validate_schema() throws Exception {
        //Given
       
        //When
       
        //Then
        Assertions.assertThat(manager).isNotNull();
    
    }

}
