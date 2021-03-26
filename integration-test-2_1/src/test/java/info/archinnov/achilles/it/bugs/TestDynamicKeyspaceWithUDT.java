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

package info.archinnov.achilles.it.bugs;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithUDTForDynamicKeyspace_Manager;
import info.archinnov.achilles.internals.entities.EntityWithUDTForDynamicKeyspace;
import info.archinnov.achilles.internals.entities.UDTWithNoKeyspace;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.SchemaNameProvider;

public class TestDynamicKeyspaceWithUDT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithUDTForDynamicKeyspace.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
            .withScript("EntityWithUDTForDynamicKeyspace/create_alternate_schema.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithUDTForDynamicKeyspace.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private EntityWithUDTForDynamicKeyspace_Manager manager = resource.getManagerFactory().forEntityWithUDTForDynamicKeyspace();
    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    private SchemaNameProvider provider = new SchemaNameProvider() {
        @Override
        public <T> String keyspaceFor(Class<T> entityClass) {
            return "dynamic_ks";
        }

        @Override
        public <T> String tableNameFor(Class<T> entityClass) {
            return "dynamic_table";
        }
    };

    /**
     * CRUD INSERT
     */
    @Test
    public void should_crud_insert_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "val");
        final EntityWithUDTForDynamicKeyspace entity = new EntityWithUDTForDynamicKeyspace(id, udt, udt);

        //When
        manager
                .crud()
                .withSchemaNameProvider(provider)
                .insert(entity)
                .execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM dynamic_ks.dynamic_table WHERE id = " + id).all();
        assertThat(rows).hasSize(1);
    }

    /**
     * CRUD FIND
     */
    @Test
    public void should_crud_find_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithUDTForDynamicKeyspace found = manager
                .crud()
                .withSchemaNameProvider(provider)
                .findById(id, udt)
                .get();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getUdt()).isEqualTo(udt);
    }

    /**
     * CRUD DELETE INSTANCE
     */
    @Test
    public void should_crud_delete_instance_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));
        final EntityWithUDTForDynamicKeyspace instance = new EntityWithUDTForDynamicKeyspace(id, udt, udt);

        //When
        manager
                .crud()
                .withSchemaNameProvider(provider)
                .delete(instance)
                .execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM dynamic_ks.dynamic_table WHERE id = " + id).all();
        assertThat(rows).hasSize(0);
    }

    /**
     * CRUDE DELETE BY ID
     */
    @Test
    public void should_crud_delete_by_id_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .crud()
                .withSchemaNameProvider(provider)
                .deleteById(id, udt)
                .execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM dynamic_ks.dynamic_table WHERE id = " + id).all();
        assertThat(rows).hasSize(0);
    }

    /**
     * CRUDE DELETE BY PARTITION
     */
    @Test
    public void should_crud_delete_by_partition_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .crud()
                .withSchemaNameProvider(provider)
                .deleteByPartitionKeys(id)
                .execute();

        //Then
        final List<Row> rows = session.execute("SELECT * FROM dynamic_ks.dynamic_table WHERE id = " + id).all();
        assertThat(rows).hasSize(0);
    }

    /**
     * DSL SELECT
     */
    @Test
    public void should_dsl_select_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithUDTForDynamicKeyspace found = manager
                .dsl()
                .select()
                .allColumns_From(provider)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .getOne();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getUdt()).isEqualTo(udt);
    }

    /**
     * DSL DELETE
     */
    @Test
    public void should_dsl_delete_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .udt()
                .from(provider)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();
        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found.isNull("udt")).isTrue();
    }

    /**
     * DSL UPDATE VALUE
     */
    @Test
    public void should_dsl_update_udt_value_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udt().Set(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final UDTValue udtValue = found.getUDTValue("udt");
        assertThat(udtValue).isNotNull();
        assertThat(udtValue.getString("\"VALUE\"")).isEqualTo("new_value");
    }

    /**
     * DSL UPDATE LIST
     */


    @Test
    public void should_dsl_update_list_append_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertList.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtList().AppendTo(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtlist FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final List<UDTValue> udtList = found.getList("udtlist", UDTValue.class);
        assertThat(udtList).isNotNull();
        assertThat(udtList).hasSize(3);
        assertThat(udtList
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toList())).containsExactly("value1", "value2", "new_value");
    }

    @Test
    public void should_dsl_update_list_prepend_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertList.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtList().PrependTo(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtlist FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final List<UDTValue> udtList = found.getList("udtlist", UDTValue.class);
        assertThat(udtList).isNotNull();
        assertThat(udtList).hasSize(3);
        assertThat(udtList
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toList())).containsExactly("new_value", "value1", "value2");
    }

    @Test
    public void should_dsl_update_list_set_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertList.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtList().Set(Arrays.asList(newUdt))
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtlist FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final List<UDTValue> udtList = found.getList("udtlist", UDTValue.class);
        assertThat(udtList).isNotNull();
        assertThat(udtList).hasSize(1);
        assertThat(udtList
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toList())).containsExactly("new_value");
    }

    /**
     * DSL UPDATE SET
     */


    @Test
    public void should_dsl_update_set_add_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertSet.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtSet().AddTo(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtset FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Set<UDTValue> udtset = found.getSet("udtset", UDTValue.class);
        assertThat(udtset).isNotNull();
        assertThat(udtset).hasSize(3);
        assertThat(udtset
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("value1", "value2", "new_value");
    }

    @Test
    public void should_dsl_update_set_remove_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace udt1 = new UDTWithNoKeyspace(id, "value1");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertSet.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtSet().RemoveFrom(udt1)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtset FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Set<UDTValue> udtset = found.getSet("udtset", UDTValue.class);
        assertThat(udtset).isNotNull();
        assertThat(udtset).hasSize(1);
        assertThat(udtset
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("value2");
    }

    /**
     * DSL UPDATE MAP KEY
     */


    @Test
    public void should_dsl_update_map_key_put_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertMapKey.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtMapKey().PutTo(newUdt, "new_value")
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtmapkey FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Map<UDTValue, String> udtMapKey = found.getMap("udtmapkey", UDTValue.class, String.class);
        assertThat(udtMapKey).isNotNull();
        assertThat(udtMapKey).hasSize(3);
        assertThat(udtMapKey
                .keySet()
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("value1", "value2", "new_value");
    }

    @Test
    public void should_dsl_update_map_key_remove_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace udt1 = new UDTWithNoKeyspace(id, "value1");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertMapKey.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtMapKey().RemoveByKey(udt1)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtmapkey FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Map<UDTValue, String> udtMapKey = found.getMap("udtmapkey", UDTValue.class, String.class);
        assertThat(udtMapKey).isNotNull();
        assertThat(udtMapKey).hasSize(1);
        assertThat(udtMapKey
                .keySet()
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("value2");
    }

    @Test
    public void should_dsl_update_map_key_set_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertMapKey.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtMapKey().Set(ImmutableMap.of(newUdt, "new_value"))
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtmapkey FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Map<UDTValue, String> udtMapKey = found.getMap("udtmapkey", UDTValue.class, String.class);
        assertThat(udtMapKey).isNotNull();
        assertThat(udtMapKey).hasSize(1);
        assertThat(udtMapKey
                .keySet()
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("new_value");
    }


    /**
     * DSL UPDATE MAP VALUE
     */

    @Test
    public void should_dsl_update_map_value_put_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertMapValue.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtMapValue().PutTo(3, newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtmapvalue FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Map<Integer, UDTValue> udtMapValue = found.getMap("udtmapvalue", Integer.class, UDTValue.class);
        assertThat(udtMapValue).isNotNull();
        assertThat(udtMapValue).hasSize(3);
        assertThat(udtMapValue
                .values()
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("value1", "value2", "new_value");
    }

    @Test
    public void should_dsl_update_map_value_remove_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertMapValue.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtMapValue().RemoveByKey(1)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtmapvalue FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Map<Integer, UDTValue> udtMapValue = found.getMap("udtmapvalue", Integer.class, UDTValue.class);
        assertThat(udtMapValue).isNotNull();
        assertThat(udtMapValue).hasSize(1);
        assertThat(udtMapValue
                .values()
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("value2");
    }

    @Test
    public void should_dsl_update_map_value_set_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertMapValue.cql", ImmutableMap.of("id", id));
        
        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udtMapValue().Set(ImmutableMap.of(3, newUdt))
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udtmapvalue FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final Map<Integer, UDTValue> udtMapValue = found.getMap("udtmapvalue", Integer.class, UDTValue.class);
        assertThat(udtMapValue).isNotNull();
        assertThat(udtMapValue).hasSize(1);
        assertThat(udtMapValue
                .values()
                .stream()
                .map(x -> x.getString("\"VALUE\""))
                .collect(toSet())).contains("new_value");
    }

    @Test
    public void should_update_using_lwt_eq_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udt().Set(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .if_Udt().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.getUDTValue("udt").getString("\"VALUE\"")).isEqualTo("new_value");
    }

    /**
     * LWT
     */


    @Test
    public void should_update_using_lwt_gte_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace comparisonUDT = new UDTWithNoKeyspace(id, "valud");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udt().Set(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .if_Udt().Gte(comparisonUDT)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.getUDTValue("udt").getString("\"VALUE\"")).isEqualTo("new_value");
    }

    @Test
    public void should_update_using_lwt_lt_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace comparisonUDT = new UDTWithNoKeyspace(id, "valuf");
        final UDTWithNoKeyspace newUdt = new UDTWithNoKeyspace(id, "new_value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(provider)
                .udt().Set(newUdt)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .if_Udt().Lt(comparisonUDT)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.getUDTValue("udt").getString("\"VALUE\"")).isEqualTo("new_value");
    }

    @Test
    public void should_delete_using_lwt_eq_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .udt()
                .from(provider)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .if_Udt().Eq(udt)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.isNull("udt")).isNotNull();
    }

    @Test
    public void should_delete_using_lwt_gte_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace comparisonUDT = new UDTWithNoKeyspace(id, "valud");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .udt()
                .from(provider)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .if_Udt().Gte(comparisonUDT)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.isNull("udt")).isNotNull();
    }

    @Test
    public void should_delete_using_lwt_lt_with_dynamic_ks() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithNoKeyspace udt = new UDTWithNoKeyspace(id, "value");
        final UDTWithNoKeyspace comparisonUDT = new UDTWithNoKeyspace(id, "valuf");
        scriptExecutor.executeScriptTemplate("EntityWithUDTForDynamicKeyspace/insertRow.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .udt()
                .from(provider)
                .where()
                .id().Eq(id)
                .clust().Eq(udt)
                .if_Udt().Lt(comparisonUDT)
                .execute();

        //Then
        final Row found = session.execute("SELECT udt FROM dynamic_ks.dynamic_table WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.isNull("udt")).isNotNull();
    }
}