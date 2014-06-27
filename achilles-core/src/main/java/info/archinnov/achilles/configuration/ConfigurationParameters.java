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

package info.archinnov.achilles.configuration;

public enum ConfigurationParameters {
    ENTITY_PACKAGES("achilles.entity.packages"),
    ENTITIES_LIST("achilles.entities.list"),

    NATIVE_SESSION("achilles.cassandra.native.session"),
    KEYSPACE_NAME("achilles.cassandra.keyspace.name"),

    OBJECT_MAPPER_FACTORY("achilles.json.object.mapper.factory"),
    OBJECT_MAPPER("achilles.json.object.mapper"),

    CONSISTENCY_LEVEL_READ_DEFAULT("achilles.consistency.read.default"),
    CONSISTENCY_LEVEL_WRITE_DEFAULT("achilles.consistency.write.default"),
    CONSISTENCY_LEVEL_READ_MAP("achilles.consistency.read.map"),
    CONSISTENCY_LEVEL_WRITE_MAP("achilles.consistency.write.map"),

    EVENT_INTERCEPTORS("achilles.event.interceptors"),

    FORCE_TABLE_CREATION("achilles.ddl.force.table.creation"),

    ENABLE_SCHEMA_UPDATE("achilles.ddl.enable.schema.update"),
    ENABLE_SCHEMA_UPDATE_FOR_TABLES("achilles.ddl.enable.schema.update.for.tables"),

    BEAN_VALIDATION_ENABLE("achilles.bean.validation.enable"),
    BEAN_VALIDATION_VALIDATOR("achilles.bean.validation.validator"),

    PREPARED_STATEMENTS_CACHE_SIZE("achilles.prepared.statements.cache.size"),

    PROXIES_WARM_UP_DISABLED("achilles.proxies.warm.up.disabled"),

    INSERT_STRATEGY("achilles.insert.strategy"),

    OSGI_CLASS_LOADER("achilles.osgi.class.loader");

    private String label;

    ConfigurationParameters(String label) {
        this.label = label;
    }

}
