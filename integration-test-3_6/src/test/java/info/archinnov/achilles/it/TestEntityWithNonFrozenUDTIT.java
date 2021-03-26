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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_6;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_6;
import info.archinnov.achilles.generated.manager.EntityWithNonFrozenUDT_Manager;
import info.archinnov.achilles.internals.entities.EntityWithNonFrozenUDT;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithNonFrozenUDTIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_6> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_6")
            .entityClassesToTruncate(EntityWithNonFrozenUDT.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_6
                    .builder(cluster)
                    .withDefaultKeyspaceName("it_3_6")
                    .withManagedEntityClasses(EntityWithNonFrozenUDT.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithNonFrozenUDT_Manager manager = resource.getManagerFactory().forEntityWithNonFrozenUDT();
    private Session session = resource.getNativeSession();

    @Test
    public void should_update_udt_fields() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithNonFrozenUDT/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .nonFrozen().val().Set("new_value")
                .nonFrozen().li().Set(Arrays.asList("3"))
                .nonFrozen().se().Set_FromJSON("[\"3\"]")
                .nonFrozen().ma().Set(ImmutableMap.of(3, "3"))
                .nonFrozen().address().Set_FromJSON("{\"street\": \"new_street\", \"number\": 100}")
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM it_3_6.non_frozen_udt WHERE id = " + id).one();

        assertThat(actual).isNotNull();

        final UDTValue nonFrozen = actual.getUDTValue("nonFrozen");

        assertThat(nonFrozen).isNotNull();
        assertThat(nonFrozen.getList("li", String.class)).containsExactly("3");
        assertThat(nonFrozen.getSet("se", String.class)).containsExactly("3");
        assertThat(nonFrozen.getMap("ma", Integer.class, String.class)).hasSize(1);
        assertThat(nonFrozen.getMap("ma", Integer.class, String.class)).containsEntry(3, "3");

        final UDTValue address = nonFrozen.getUDTValue("address");

        assertThat(address).isNotNull();
        assertThat(address.getString("street")).isEqualTo("new_street");
        assertThat(address.getInt("number")).isEqualTo(100);
    }
}
