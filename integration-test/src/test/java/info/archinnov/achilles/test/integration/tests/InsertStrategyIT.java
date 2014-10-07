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

import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.configuration.ConfigurationParameters.INSERT_STRATEGY;
import static info.archinnov.achilles.test.integration.AchillesInternalCQLResource.CLEAN_DATA_FILES_PROPERTY;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static info.archinnov.achilles.type.InsertStrategy.NOT_NULL_FIELDS;
import static org.fest.assertions.api.Assertions.assertThat;

import com.google.common.base.Optional;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.EntityWithNotNullInsertStrategy;

public class InsertStrategyIT {

    final boolean cleanDataFiles = Boolean.parseBoolean(fromNullable(System.getProperty(CLEAN_DATA_FILES_PROPERTY)).or("true"));

    private PersistenceManager manager1 = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class, EntityWithNotNullInsertStrategy.class)
            .withKeyspaceName("ALL_FIELDS_INSERT")
            .withAchillesConfigParams(ImmutableMap.<ConfigurationParameters, Object>of(INSERT_STRATEGY, ALL_FIELDS))
            .cleanDataFilesAtStartup(cleanDataFiles)
            .buildPersistenceManager();

    private PersistenceManager manager2 = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class)
            .withKeyspaceName("NOT_NULL_FIELDS_INSERT")
            .withAchillesConfigParams(ImmutableMap.<ConfigurationParameters, Object>of(INSERT_STRATEGY, NOT_NULL_FIELDS))
            .cleanDataFilesAtStartup(cleanDataFiles)
            .buildPersistenceManager();


    @Test
    public void should_insert_all_fields() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        CompleteBean entity = new CompleteBean();
        entity.setId(id);
        entity.setName("John");
        entity.setAge(33L);

        //When
        manager1.insert(entity);
        entity.setName("Helen");
        entity.setAge(null);

        manager1.insert(entity);

        //Then
        final CompleteBean found = manager1.find(CompleteBean.class, id);

        assertThat(found.getName()).isEqualTo("Helen");
        assertThat(found.getAge()).isNull();
    }

    @Test
    public void should_insert_not_null_field_overriding_global_config() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        EntityWithNotNullInsertStrategy entity = new EntityWithNotNullInsertStrategy();
        entity.setId(id);
        entity.setName("Helen");
        entity.setLabel("label");

        //When
        manager1.insert(entity);
        entity.setLabel(null);

        manager1.insert(entity);

        //Then
        final EntityWithNotNullInsertStrategy found = manager1.find(EntityWithNotNullInsertStrategy.class, id);

        assertThat(found.getName()).isEqualTo("Helen");
        assertThat(found.getLabel()).isEqualTo("label");
    }

    @Test
    public void should_insert_only_fields_that_are_not_null() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        CompleteBean entity = new CompleteBean();
        entity.setId(id);
        entity.setName("John");
        entity.setAge(33L);

        //When
        manager2.insert(entity);
        entity.setName("Helen");
        entity.setAge(null);

        manager2.insert(entity);

        //Then
        final CompleteBean found = manager2.find(CompleteBean.class, id);

        assertThat(found.getName()).isEqualTo("Helen");
        assertThat(found.getAge()).isEqualTo(33L);

    }
}
