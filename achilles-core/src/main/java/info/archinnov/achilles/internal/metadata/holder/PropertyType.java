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
package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.metadata.util.PropertyTypeExclude;
import info.archinnov.achilles.internal.metadata.util.PropertyTypeFilter;

public enum PropertyType {

    PARTITION_KEY, //
    COMPOUND_PRIMARY_KEY, //
    SIMPLE, //
    LIST, //
    SET, //
    MAP, //
    COUNTER;


    public boolean isCounter() {
        return (this == COUNTER);
    }

    public boolean isPrimaryKey() {
        return this == PARTITION_KEY || this == COMPOUND_PRIMARY_KEY;
    }

    public boolean isCompoundPK() {
        return this == COMPOUND_PRIMARY_KEY;
    }

    public boolean isValidClusteredValueType() {
        return (this == SIMPLE || this == COUNTER);
    }

    public boolean isCollectionAndMap() {
        return (this == LIST || this == SET || this == MAP);
    }

    public static final PropertyTypeFilter COUNTER_TYPE = new PropertyTypeFilter(COUNTER);
    public static final PropertyTypeFilter COMPOUND_PK_TYPE = new PropertyTypeFilter(COMPOUND_PRIMARY_KEY);

    public static final PropertyTypeExclude EXCLUDE_ID_TYPES = new PropertyTypeExclude(PARTITION_KEY, COMPOUND_PRIMARY_KEY);
    public static final PropertyTypeExclude EXCLUDE_COMPOUND_PK_TYPE = new PropertyTypeExclude(COMPOUND_PRIMARY_KEY);

    public static final PropertyTypeExclude EXCLUDE_COUNTER_TYPE = new PropertyTypeExclude(COUNTER);

    public static final PropertyTypeExclude EXCLUDE_PK_AND_COUNTER_TYPE = new PropertyTypeExclude(PARTITION_KEY, COMPOUND_PRIMARY_KEY, COUNTER);
}
