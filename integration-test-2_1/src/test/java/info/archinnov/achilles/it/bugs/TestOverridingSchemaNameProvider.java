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


import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithUDTs_Manager;
import info.archinnov.achilles.internals.entities.EntityWithUDTs;
import info.archinnov.achilles.internals.entities.SimpleUDTWithNoKeyspace;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.tuples.Tuple2;

public class TestOverridingSchemaNameProvider {

    private final SchemaNameProvider defaultProvider = new SchemaNameProvider() {
        @Override
        public <T> String keyspaceFor(Class<T> entityClass) {
            return "new_ks";
        }

        @Override
        public <T> String tableNameFor(Class<T> entityClass) {
            return "static_table";
        }
    };

    private final SchemaNameProvider runtimeProvider = new SchemaNameProvider() {
        @Override
        public <T> String keyspaceFor(Class<T> entityClass) {
            return "overriden_schema_name";
        }

        @Override
        public <T> String tableNameFor(Class<T> entityClass) {
            return "entitywithudts";
        }
    };

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithUDTs.class)
            .truncateBeforeAndAfterTest()
            .withScript("EntityWithUDTs/createSchema.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithUDTs.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withSchemaNameProvider(defaultProvider)
                    .withDefaultKeyspaceName("new_ks")
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithUDTs_Manager manager = resource.getManagerFactory().forEntityWithUDTs();
    private Session session = resource.getNativeSession();

    @Test
    public void should_insert_with_default_schema_name_provider() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final SimpleUDTWithNoKeyspace udt = new SimpleUDTWithNoKeyspace(id, "val");
        final EntityWithUDTs entity = new EntityWithUDTs();
        entity.setId(id);
        entity.setListUDT(Arrays.asList(udt));
        entity.setSetUDT(Sets.newHashSet(udt));
        entity.setMapUDT(ImmutableMap.of(udt, udt));
        entity.setTupleUDT(Tuple2.of(1, udt));
        entity.setOptionalUDT(Optional.of(udt));

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT * FROM new_ks.entity_with_udts WHERE id = " + id).all();
        assertThat(all).hasSize(1);
    }

    @Test
    public void should_insert_with_overriden_schema_name_provider() throws Exception {
        //Given


        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final SimpleUDTWithNoKeyspace udt = new SimpleUDTWithNoKeyspace(id, "val");
        final EntityWithUDTs entity = new EntityWithUDTs();
        entity.setId(id);
        entity.setListUDT(Arrays.asList(udt));
        entity.setSetUDT(Sets.newHashSet(udt));
        entity.setMapUDT(ImmutableMap.of(udt, udt));
        entity.setTupleUDT(Tuple2.of(1, udt));
        entity.setOptionalUDT(Optional.of(udt));

        //When
        manager
                .crud()
                .withSchemaNameProvider(runtimeProvider)
                .insert(entity)
                .execute();

        //Then
        final List<Row> all = session.execute("SELECT * FROM overriden_schema_name.entitywithudts WHERE id = " + id).all();
        assertThat(all).hasSize(1);
    }

    @Test
    public void should_find_using_default_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_default_schema.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithUDTs found = manager
                .crud()
                .findById(id)
                .get();

        //Then
        assertThat(found).isNotNull();
    }

    @Test
    public void should_find_using_runtime_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_runtime_schema.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithUDTs found = manager
                .crud()
                .withSchemaNameProvider(runtimeProvider)
                .findById(id)
                .get();

        //Then
        assertThat(found).isNotNull();
    }

    @Test
    public void should_delete_using_default_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_default_schema.cql", ImmutableMap.of("id", id));

        //When
        manager
                .crud()
                .deleteById(id)
                .execute();

        //Then
        final Row found = session.execute("SELECT * FROM new_ks.entity_with_udts WHERE id = " + id).one();
        assertThat(found).isNull();
    }

    @Test
    public void should_delete_using_runtime_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_runtime_schema.cql", ImmutableMap.of("id", id));

        //When
        manager
                .crud()
                .withSchemaNameProvider(runtimeProvider)
                .deleteById(id)
                .execute();

        //Then
        final Row found = session.execute("SELECT * FROM overriden_schema_name.entitywithudts WHERE id = " + id).one();
        assertThat(found).isNull();
    }

    @Test
    public void should_dsl_select_using_default_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_default_schema.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithUDTs found = manager
                .dsl()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .getOne();

        //Then
        assertThat(found).isNotNull();
    }

    @Test
    public void should_dsl_select_using_runtime_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_runtime_schema.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithUDTs found = manager
                .dsl()
                .select()
                .allColumns_From(runtimeProvider)
                .where()
                .id().Eq(id)
                .getOne();

        //Then
        assertThat(found).isNotNull();
    }

    @Test
    public void should_dsl_update_using_default_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final SimpleUDTWithNoKeyspace newUDT = new SimpleUDTWithNoKeyspace(id, "new_val");
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_default_schema.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .fromBaseTable()
                .optionalUDT().Set(Optional.of(newUDT))
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row found = session.execute("SELECT optionaludt FROM new_ks.entity_with_udts WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.getUDTValue("optionaludt").getString("value")).isEqualTo("new_val");
    }

    @Test
    public void should_dsl_update_using_runtime_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final SimpleUDTWithNoKeyspace newUDT = new SimpleUDTWithNoKeyspace(id, "new_val");
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_runtime_schema.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .update()
                .from(runtimeProvider)
                .optionalUDT().Set(Optional.of(newUDT))
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row found = session.execute("SELECT optionaludt FROM overriden_schema_name.entitywithudts WHERE id = " + id).one();
        assertThat(found).isNotNull();
        assertThat(found.getUDTValue("optionaludt").getString("value")).isEqualTo("new_val");
    }

    @Test
    public void should_dsl_delete_using_default_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_default_schema.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .allColumns_FromBaseTable()
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row found = session.execute("SELECT * FROM new_ks.entity_with_udts WHERE id = " + id).one();
        assertThat(found).isNull();
    }

    @Test
    public void should_dsl_delete_using_runtime_schema_name() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithUDTs/insertRow_runtime_schema.cql", ImmutableMap.of("id", id));

        //When
        manager
                .dsl()
                .delete()
                .allColumns_From(runtimeProvider)
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final Row found = session.execute("SELECT * FROM overriden_schema_name.entitywithudts WHERE id = " + id).one();
        assertThat(found).isNull();
    }
}
