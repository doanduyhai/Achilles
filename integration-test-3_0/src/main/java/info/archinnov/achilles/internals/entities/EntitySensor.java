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

package info.archinnov.achilles.internals.entities;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.type.strategy.NamingStrategy;

@Table(table = "sensor")
@Strategy(naming = NamingStrategy.SNAKE_CASE)
public class EntitySensor {

    @PartitionKey
    private Long sensorId;

    @ClusteringColumn
    private Long date;

    @Enumerated
    @Column
    private SensorType type;

    @Column
    private Double value;

    public EntitySensor(Long sensorId, Long date, @Enumerated SensorType type, Double value) {
        this.sensorId = sensorId;
        this.date = date;
        this.type = type;
        this.value = value;
    }

    public EntitySensor() {
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

    public SensorType getType() {
        return type;
    }

    public void setType(SensorType type) {
        this.type = type;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public enum SensorType {
        TEMPERATURE, PRESSURE, GPS
    }
}
