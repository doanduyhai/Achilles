/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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
package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.persistence.PersistenceManagerFactory.PersistenceManagerFactoryBuilder;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithClassLevelConstraint;
import info.archinnov.achilles.test.integration.entity.EntityWithFieldLevelConstraint;
import info.archinnov.achilles.test.integration.entity.EntityWithGroupConstraint;
import info.archinnov.achilles.test.integration.entity.EntityWithGroupConstraint.CustomValidationInterceptor;
import info.archinnov.achilles.test.integration.entity.EntityWithPropertyLevelConstraint;

public class BeanValidationIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
            EntityWithClassLevelConstraint.TABLE_NAME, EntityWithFieldLevelConstraint.TABLE_NAME,
            EntityWithPropertyLevelConstraint.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_validate_entity_constrained_on_field() throws Exception {
        // Given
        Long id = RandomUtils.nextLong();
        EntityWithFieldLevelConstraint entity = new EntityWithFieldLevelConstraint();
        entity.setId(id);
        entity.setName("name");
        manager.persist(entity);

        // When
        EntityWithFieldLevelConstraint found = manager.find(EntityWithFieldLevelConstraint.class, id);

        // Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("name");
    }

    @Test
    public void should_validate_entity_constrained_on_property() throws Exception {
        // Given
        Long id = RandomUtils.nextLong();
        EntityWithPropertyLevelConstraint entity = new EntityWithPropertyLevelConstraint();
        entity.setId(id);
        entity.setName("name");
        manager.persist(entity);

        // When
        EntityWithPropertyLevelConstraint found = manager.find(EntityWithPropertyLevelConstraint.class, id);

        // Then
        assertThat(found).isNotNull();
        assertThat(found.getName()).isEqualTo("name");
    }

    @Test
    public void should_validate_entity_constrained_on_class() throws Exception {
        // Given
        Long id = RandomUtils.nextLong();
        EntityWithClassLevelConstraint entity = new EntityWithClassLevelConstraint();
        entity.setId(id);
        entity.setFirstname("firstname");
        entity.setLastname("lastname");
        manager.persist(entity);

        // When
        EntityWithClassLevelConstraint found = manager.find(EntityWithClassLevelConstraint.class, id);

        // Then
        assertThat(found).isNotNull();
        assertThat(found.getFirstname()).isEqualTo("firstname");
        assertThat(found.getLastname()).isEqualTo("lastname");
    }

    @Test
    public void should_error_on_invalid_field_persist() throws Exception {
        // Given
        boolean exceptionRaised = false;
        Long id = RandomUtils.nextLong();
        EntityWithFieldLevelConstraint entity = new EntityWithFieldLevelConstraint();
        entity.setId(id);

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tproperty 'name' of class '");
        errorMessage.append(EntityWithFieldLevelConstraint.class.getCanonicalName()).append("'");

        try {
            // When
            manager.persist(entity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }

    @Test
    public void should_error_on_invalid_field_update() throws Exception {
        // Given
        boolean exceptionRaised = false;
        Long id = RandomUtils.nextLong();
        EntityWithFieldLevelConstraint entity = new EntityWithFieldLevelConstraint();
        entity.setId(id);
        entity.setName("name");

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tproperty 'name' of class '");
        errorMessage.append(EntityWithFieldLevelConstraint.class.getCanonicalName()).append("'");
        EntityWithFieldLevelConstraint managedEntity = manager.persist(entity);

        try {
            // When
            managedEntity.setName(null);
            manager.update(managedEntity);
        } catch (AchillesBeanValidationException ex) {

            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }

    @Test
    public void should_error_on_invalid_property_persist() throws Exception {
        // Given
        boolean exceptionRaised = false;
        Long id = RandomUtils.nextLong();
        EntityWithPropertyLevelConstraint entity = new EntityWithPropertyLevelConstraint();
        entity.setId(id);

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tproperty 'name' of class '");
        errorMessage.append(EntityWithPropertyLevelConstraint.class.getCanonicalName()).append("'");

        try {
            // When
            manager.persist(entity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }

    @Test
    public void should_error_on_invalid_property_update() throws Exception {
        // Given
        boolean exceptionRaised = false;
        Long id = RandomUtils.nextLong();
        EntityWithPropertyLevelConstraint entity = new EntityWithPropertyLevelConstraint();
        entity.setId(id);
        entity.setName("name");

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tproperty 'name' of class '");
        errorMessage.append(EntityWithPropertyLevelConstraint.class.getCanonicalName()).append("'");
        EntityWithPropertyLevelConstraint managedEntity = manager.persist(entity);

        try {
            // When
            managedEntity.setName(null);
            manager.update(managedEntity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }

    @Test
    public void should_error_on_invalid_class_persist() throws Exception {
        // Given
        boolean exceptionRaised = false;
        Long id = RandomUtils.nextLong();
        EntityWithClassLevelConstraint entity = new EntityWithClassLevelConstraint();
        entity.setId(id);
        entity.setFirstname("fn");

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tfirstname and lastname should not be blank for class '");
        errorMessage.append(EntityWithClassLevelConstraint.class.getCanonicalName()).append("'");

        try {
            // When
            manager.persist(entity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }

    @Test
    public void should_error_on_invalid_class_update() throws Exception {
        // Given
        boolean exceptionRaised = false;
        Long id = RandomUtils.nextLong();
        EntityWithClassLevelConstraint entity = new EntityWithClassLevelConstraint();
        entity.setId(id);
        entity.setFirstname("fn");
        entity.setLastname("ln");

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tfirstname and lastname should not be blank for class '");
        errorMessage.append(EntityWithClassLevelConstraint.class.getCanonicalName()).append("'");

        EntityWithClassLevelConstraint managedEntity = manager.persist(entity);

        try {
            // When
            managedEntity.setFirstname(null);
            manager.update(managedEntity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }

    @Test
    public void should_use_custom_bean_validator_interceptor() throws Exception {
        // Given
        Long id = RandomUtils.nextLong();
        boolean exceptionRaised = false;

        Session nativeSession = this.manager.getNativeSession();
        Cluster cluster = nativeSession.getCluster();
        CustomValidationInterceptor interceptor = new CustomValidationInterceptor();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigurationParameters.NATIVE_SESSION_PARAM, nativeSession);
        configMap.put(ConfigurationParameters.ENTITIES_LIST_PARAM, Arrays.asList(EntityWithGroupConstraint.class));
        configMap.put(ConfigurationParameters.BEAN_VALIDATION_ENABLE, true);
        configMap.put(ConfigurationParameters.EVENT_INTERCEPTORS_PARAM, Arrays.asList(interceptor));
        configMap.put(ConfigurationParameters.KEYSPACE_NAME_PARAM, "achilles_test");
        PersistenceManagerFactory managerFactory = PersistenceManagerFactoryBuilder.build(cluster, configMap);
        PersistenceManager manager = managerFactory.createPersistenceManager();

        EntityWithGroupConstraint entity = new EntityWithGroupConstraint();
        entity.setId(id);

        StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
        errorMessage.append("\tproperty 'name' of class '");
        errorMessage.append(EntityWithGroupConstraint.class.getCanonicalName()).append("'");

        try {
            // When
            manager.persist(entity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).contains(errorMessage.toString());
            exceptionRaised = true;
        }
        assertThat(exceptionRaised).isTrue();
    }
}
