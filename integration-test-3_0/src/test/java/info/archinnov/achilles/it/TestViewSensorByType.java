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

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.SimpleStatement;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_0;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_0;
import info.archinnov.achilles.generated.manager.EntitySensor_Manager;
import info.archinnov.achilles.generated.manager.ViewSensorByType_Manager;
import info.archinnov.achilles.internals.entities.EntitySensor;
import info.archinnov.achilles.internals.entities.EntitySensor.SensorType;
import info.archinnov.achilles.internals.views.ViewSensorByType;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.type.TypedMap;

public class TestViewSensorByType {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_0> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntitySensor.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_0
                    .builder(cluster)
                    .withManagedEntityClasses(EntitySensor.class, ViewSensorByType.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private EntitySensor_Manager sensorManager = resource.getManagerFactory().forEntitySensor();
    private ViewSensorByType_Manager viewSensorManager = resource.getManagerFactory().forViewSensorByType();


    @Test
    public void should_find_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        sensorManager.crud().insert(new EntitySensor(id, 20160215L, SensorType.TEMPERATURE, 18.34d)).execute();

        //When
        final ViewSensorByType found = viewSensorManager.crud().findById(SensorType.TEMPERATURE, id, 20160215L).get();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getValue()).isEqualTo(18.34d);
    }

    @Test
    public void should_select_by_slice() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        sensorManager.crud().insert(new EntitySensor(id, 20160215L, SensorType.TEMPERATURE, 18.34d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160216L, SensorType.PRESSURE, 1.05d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160217L, SensorType.TEMPERATURE, 21.5d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160218L, SensorType.PRESSURE, 1.04d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160219L, SensorType.TEMPERATURE, 17.8d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160220L, SensorType.PRESSURE, 1.03d)).execute();

        //When
        final List<ViewSensorByType> found = viewSensorManager
                .dsl()
                .select()
                .value()
                .fromBaseTable()
                .where()
                .type().Eq(SensorType.TEMPERATURE)
                .sensorId().Eq(id)
                .date().Gte_And_Lte(20160215L, 20160218L)
                .getList();


        //Then
        assertThat(found).hasSize(2);
        assertThat(found.get(0).getValue()).isEqualTo(18.34d);
        assertThat(found.get(1).getValue()).isEqualTo(21.5d);
    }

    @Test
    public void should_typed_query() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        sensorManager.crud().insert(new EntitySensor(id, 20160215L, SensorType.TEMPERATURE, 18.34d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160216L, SensorType.PRESSURE, 1.05d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160217L, SensorType.TEMPERATURE, 21.5d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160218L, SensorType.PRESSURE, 1.04d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160219L, SensorType.TEMPERATURE, 17.8d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160220L, SensorType.PRESSURE, 1.03d)).execute();

        //When
        final List<ViewSensorByType> found = viewSensorManager
                .raw()
                .typedQueryForSelect(new SimpleStatement("SELECT value FROM sensor_by_type " +
                        "WHERE type='PRESSURE' " +
                        "AND sensor_id=" + id + " " +
                        "AND date>=20160215 " +
                        "AND date<20160220")).getList();
        //Then
        assertThat(found).hasSize(2);
        assertThat(found.get(0).getValue()).isEqualTo(1.05d);
        assertThat(found.get(1).getValue()).isEqualTo(1.04d);
    }

    @Test
    public void should_native_query() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        sensorManager.crud().insert(new EntitySensor(id, 20160215L, SensorType.TEMPERATURE, 18.34d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160216L, SensorType.PRESSURE, 1.05d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160217L, SensorType.TEMPERATURE, 21.5d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160218L, SensorType.PRESSURE, 1.04d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160219L, SensorType.TEMPERATURE, 17.8d)).execute();
        sensorManager.crud().insert(new EntitySensor(id, 20160220L, SensorType.PRESSURE, 1.03d)).execute();

        //When
        final List<TypedMap> found = viewSensorManager
                .raw()
                .nativeQuery(new SimpleStatement("SELECT value FROM sensor_by_type " +
                        "WHERE type=:type AND sensor_id=:id AND date>=:date1 AND date<:date2"),
                        "TEMPERATURE", id, 20160215L, 20160219L)
                .getTypedMaps();
        //Then
        assertThat(found).hasSize(2);
        assertThat(found.get(0).<Double>getTyped("value")).isEqualTo(18.34d);
        assertThat(found.get(1).<Double>getTyped("value")).isEqualTo(21.5d);
    }

    @Test
    public void should_fail_if_mutation_statement_for_view() throws Exception {
        //Given
        exception.expect(AchillesException.class);
        exception.expectMessage("Statement provided for the materialized view " +
                "'info.archinnov.achilles.internals.views.ViewSensorByType' should be an SELECT statement");

        //When
        viewSensorManager
                .raw()
                .nativeQuery(new SimpleStatement("INSERT INTO sensor_by_type(type, sensor_id, date, value) " +
                        "VALUES('PRESSURE',123, 20160215, 12.8)"))
                .getTypedMap();

        //Then

    }
}
