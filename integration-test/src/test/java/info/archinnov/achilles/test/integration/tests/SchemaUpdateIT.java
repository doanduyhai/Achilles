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

import static info.archinnov.achilles.embedded.ServerStarter.CASSANDRA_EMBEDDED;
import static info.archinnov.achilles.persistence.PersistenceManagerFactory.PersistenceManagerFactoryBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.TimeUUID;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.utils.UUIDGen;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

public class SchemaUpdateIT {

    @Test
    public void should_allow_dynamic_schema_update() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final Session session = CassandraEmbeddedServerBuilder
                .noEntityPackages().withKeyspaceName("schema_dynamic_update")
                .cleanDataFilesAtStartup(true)
                .buildNativeSessionOnly();
        session.execute("DROP TABLE IF EXISTS new_simple_field");
        session.execute("DROP INDEX IF EXISTS field_index");
        session.execute("CREATE TABLE new_simple_field(id bigint PRIMARY KEY, existing_field text)");
        session.execute("CREATE INDEX field_index ON schema_dynamic_update.new_simple_field(existing_field)");

        //When
        final PersistenceManagerFactory pmf = PersistenceManagerFactoryBuilder.builder(session.getCluster())
                .withNativeSession(session)
                .withEntities(Arrays.<Class<?>>asList(EntityWithNewSimpleField.class))
                .enableSchemaUpdate(false)
                .enableSchemaUpdateForTables(ImmutableMap.of("schema_dynamic_update.new_simple_field", true))
                .relaxIndexValidation(true)
                .build();

        final PersistenceManager pm = pmf.createPersistenceManager();
        pm.insert(new EntityWithNewSimpleField(id, "existing", "new"));

        //Then
        final EntityWithNewSimpleField found = pm.find(EntityWithNewSimpleField.class, id);

        assertThat(found).isNotNull();
        assertThat(found.getUnmappedField()).isEqualTo("UNMAPPED");

        assertThat(pm.find(EntityWithNewSimpleField.class, id).getUnmappedField()).isEqualTo("UNMAPPED");

        session.close();
    }

    @Test
    public void should_allow_dynamic_schema_update_for_cluster_counter() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        UUID date = UUIDGen.getTimeUUID();

        final Session session = CassandraEmbeddedServerBuilder
                .withEntities(Arrays.<Class<?>>asList(CompleteBean.class))
                .withKeyspaceName("schema_dynamic_update_counter")
                .cleanDataFilesAtStartup(true)
                .buildNativeSessionOnly();
        session.execute("DROP TABLE IF EXISTS new_counter_field");
        session.execute("CREATE TABLE new_counter_field(id bigint, date timeuuid, existing_counter counter, PRIMARY KEY(id,date))");

        //When
        final PersistenceManagerFactory pmf = PersistenceManagerFactoryBuilder.builder(session.getCluster())
                .withEntities(Arrays.<Class<?>>asList(ClusteredCounterEntityWithNewCounterField.class))
                .enableSchemaUpdate(true)
                .build();

        final PersistenceManager pm = pmf.createPersistenceManager();
        pm.insert(new ClusteredCounterEntityWithNewCounterField(id, date, CounterBuilder.incr(12L)));

        //Then
        assertThat(pm.find(ClusteredCounterEntityWithNewCounterField.class, new ClusteredCounterEntityWithNewCounterField.Compound(id, date))).isNotNull();

        session.close();
    }

    @Test(expected = AchillesInvalidTableException.class)
    public void should_fail_bootstrapping_if_schema_update_forbidden_for_entity() throws Exception {
        //Given
        final Session session = CassandraEmbeddedServerBuilder
                .noEntityPackages().withKeyspaceName("schema_dynamic_update_fail")
                .cleanDataFilesAtStartup(true)
                .buildNativeSessionOnly();
        session.execute("DROP TABLE IF EXISTS new_simple_field");
        session.execute("CREATE TABLE new_simple_field(id bigint PRIMARY KEY, existing_field text)");

        //When
        PersistenceManagerFactoryBuilder.builder(session.getCluster())
                .withEntities(Arrays.<Class<?>>asList(EntityWithNewSimpleField.class))
                .withKeyspaceName("schema_dynamic_update_fail")
                .enableSchemaUpdate(true)
                .enableSchemaUpdateForTables(ImmutableMap.of("schema_dynamic_update_fail.new_simple_field", false))
                .build();

        session.close();
    }

    @Test(expected = AchillesInvalidTableException.class)
    public void should_fail_bootstrapping_if_index_validation_fails() throws Exception {
        //Given
        final Session session = CassandraEmbeddedServerBuilder
                .noEntityPackages().withKeyspaceName("schema_dynamic_update")
                .cleanDataFilesAtStartup(true)
                .buildNativeSessionOnly();

        //When
        PersistenceManagerFactoryBuilder.builder(session.getCluster())
                .withEntities(Arrays.<Class<?>>asList(EntityWithNewSimpleField.class))
                .enableSchemaUpdate(true)
                .enableSchemaUpdateForTables(ImmutableMap.of("schema_dynamic_update.new_simple_field", false))
                .relaxIndexValidation(false)
                .build();

        session.close();
    }

    @Entity(keyspace = "schema_dynamic_update", table = "new_simple_field")
    public static class EntityWithNewSimpleField {

        @Id
        private Long id;

        @Column(name = "existing_field")
        private String existingField;

        private String unmappedField = "UNMAPPED";

        @Column(name = "new_field")
        private String newField;

        @Column(name = "new_list")
        private List<String> newList;

        @Column(name = "new_set")
        private Set<String> newSet;

        @Column(name = "new_map")
        private Map<Integer, String> newMap;

        public EntityWithNewSimpleField() {
        }

        public EntityWithNewSimpleField(Long id, String existingField, String newField) {
            this.id = id;
            this.existingField = existingField;
            this.newField = newField;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getExistingField() {
            return existingField;
        }

        public void setExistingField(String existingField) {
            this.existingField = existingField;
        }

        public String getUnmappedField() {
            return unmappedField;
        }

        public void setUnmappedField(String unmappedField) {
            this.unmappedField = unmappedField;
        }

        public String getNewField() {
            return newField;
        }

        public void setNewField(String newField) {
            this.newField = newField;
        }

        public List<String> getNewList() {
            return newList;
        }

        public void setNewList(List<String> newList) {
            this.newList = newList;
        }

        public Set<String> getNewSet() {
            return newSet;
        }

        public void setNewSet(Set<String> newSet) {
            this.newSet = newSet;
        }

        public Map<Integer, String> getNewMap() {
            return newMap;
        }

        public void setNewMap(Map<Integer, String> newMap) {
            this.newMap = newMap;
        }
    }

    @Entity(keyspace = "schema_dynamic_update_counter", table = "new_counter_field")
    public static class ClusteredCounterEntityWithNewCounterField {

        @EmbeddedId
        private Compound id;

        @Column(name = "existing_counter")
        private Counter existingCounter;

        @Column(name = "new_counter_1")
        private Counter newCounter1;

        @Column(name = "new_counter_2")
        private Counter newCounter2;

        public ClusteredCounterEntityWithNewCounterField() {
        }

        public ClusteredCounterEntityWithNewCounterField(Long id, UUID date, Counter existingCounter) {
            this.id = new Compound(id, date);
            this.existingCounter = existingCounter;
        }

        public Compound getId() {
            return id;
        }

        public void setId(Compound id) {
            this.id = id;
        }

        public Counter getExistingCounter() {
            return existingCounter;
        }

        public void setExistingCounter(Counter existingCounter) {
            this.existingCounter = existingCounter;
        }

        public Counter getNewCounter1() {
            return newCounter1;
        }

        public void setNewCounter1(Counter newCounter1) {
            this.newCounter1 = newCounter1;
        }

        public Counter getNewCounter2() {
            return newCounter2;
        }

        public void setNewCounter2(Counter newCounter2) {
            this.newCounter2 = newCounter2;
        }

        public static class Compound {

            @Order(1)
            private Long id;

            @Order(2)
            @TimeUUID
            private UUID date;

            public Compound() {
            }

            public Compound(Long id, UUID date) {
                this.id = id;
                this.date = date;
            }

            public Long getId() {
                return id;
            }

            public void setId(Long id) {
                this.id = id;
            }

            public UUID getDate() {
                return date;
            }

            public void setDate(UUID date) {
                this.date = date;
            }
        }

    }

    @Entity(table = "entity_with_index")
    public static class EntityWithIndex {

        @Id
        private Long id;

        @Column
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
