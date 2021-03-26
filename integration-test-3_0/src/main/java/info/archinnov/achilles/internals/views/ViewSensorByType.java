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

package info.archinnov.achilles.internals.views;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.entities.EntitySensor;
import info.archinnov.achilles.internals.entities.EntitySensor.SensorType;
import info.archinnov.achilles.type.strategy.NamingStrategy;

@MaterializedView(baseEntity = EntitySensor.class, view = "sensor_by_type")
@Strategy(naming = NamingStrategy.SNAKE_CASE)
public class ViewSensorByType {

    @PartitionKey
    @Enumerated
    private SensorType type;

    @ClusteringColumn(1)
    private Long sensorId;

    @ClusteringColumn(2)
    private Long date;

    @Column
    private Double value;

    public SensorType getType() {
        return type;
    }

    public void setType(SensorType type) {
        this.type = type;
    }

    public Long getSensorId() {
        return sensorId;
    }

    public void setSensorId(Long sensorId) {
        this.sensorId = sensorId;
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
