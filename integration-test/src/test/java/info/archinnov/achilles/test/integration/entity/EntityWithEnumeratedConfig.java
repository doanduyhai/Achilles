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
package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.Enumerated.Encoding;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.util.List;
import java.util.Map;

import static info.archinnov.achilles.test.integration.entity.EntityWithEnumeratedConfig.TABLE_NAME;

@Entity(table = TABLE_NAME)
public class EntityWithEnumeratedConfig {
    public static final String TABLE_NAME = "entity_with_enumerated_config";

    @Id
    private Long id;

    @Column(name = "consistency_level")
    @Enumerated(Encoding.ORDINAL)
    private ConsistencyLevel consistencyLevel;

    @Column(name = "element_types")
    @Enumerated(Encoding.NAME)
    private List<ElementType> elementTypes;

    @Column(name = "retention_policies")
    @Enumerated(key = Encoding.ORDINAL, value = Encoding.NAME)
    private Map<RetentionPolicy, ElementType> retentionPolicies;

    public EntityWithEnumeratedConfig() {
    }

    public EntityWithEnumeratedConfig(Long id, ConsistencyLevel consistencyLevel, List<ElementType> elementTypes, Map<RetentionPolicy, ElementType> retentionPolicies) {
        this.id = id;
        this.consistencyLevel = consistencyLevel;
        this.elementTypes = elementTypes;
        this.retentionPolicies = retentionPolicies;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public List<ElementType> getElementTypes() {
        return elementTypes;
    }

    public void setElementTypes(List<ElementType> elementTypes) {
        this.elementTypes = elementTypes;
    }

    public Map<RetentionPolicy, ElementType> getRetentionPolicies() {
        return retentionPolicies;
    }

    public void setRetentionPolicies(Map<RetentionPolicy, ElementType> retentionPolicies) {
        this.retentionPolicies = retentionPolicies;
    }
}
