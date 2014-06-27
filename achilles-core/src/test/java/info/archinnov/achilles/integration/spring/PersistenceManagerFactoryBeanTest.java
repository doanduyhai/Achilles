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
package info.archinnov.achilles.integration.spring;

import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import javax.validation.Validator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.InsertStrategy;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerFactoryBeanTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PersistenceManagerFactoryBean factory;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ObjectMapperFactory objectMapperFactory;

    @Mock
    private Cluster cluster;

    @Mock
    private Session session;

    @Before
    public void setUp() {
        factory = new PersistenceManagerFactoryBean();
    }


    @Test
    public void should_exception_when_no_keyspace_name_set() throws Exception {
        factory.setEntityPackages("com.test");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Keyspace name should be provided");
        factory.initialize();
    }

    @Test
    public void should_build_with_minimum_parameters() throws Exception {
        when(cluster.connect("keyspace")).thenThrow(IllegalStateException.class);
        factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
        factory.setKeyspaceName("keyspace");
        factory.setCluster(cluster);
        exception.expect(IllegalStateException.class);

        factory.initialize();
    }

    @Test
    public void should_build_with_all_parameters() throws Exception {
        when(cluster.connect("keyspace")).thenThrow(IllegalStateException.class);
        factory.setCluster(cluster);

        factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
        factory.setKeyspaceName("keyspace");

        factory.setObjectMapper(objectMapper);
        factory.setObjectMapperFactory(objectMapperFactory);
        factory.setConsistencyLevelReadDefault(ONE);
        factory.setConsistencyLevelWriteDefault(ONE);
        factory.setConsistencyLevelReadMap(ImmutableMap.of("entity", ONE));
        factory.setConsistencyLevelWriteMap(ImmutableMap.of("entity", ONE));
        factory.setForceTableCreation(true);
        factory.setEntityList(Arrays.<Class<?>>asList(CompleteBean.class));
        factory.setEnableBeanValidation(true);
        factory.setBeanValidator(Mockito.mock(Validator.class));
        factory.setPreparedStatementCacheSize(100);
        factory.setDisableProxiesWarmUp(true);
        factory.setInsertStrategy(InsertStrategy.NOT_NULL_FIELDS);
        factory.setOsgiClassLoader(this.getClass().getClassLoader());

        exception.expect(IllegalStateException.class);

        factory.initialize();
    }
}
