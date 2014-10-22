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

package info.archinnov.achilles.internal.context.facade;

import java.util.List;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.type.ConsistencyLevel;

public interface EntityOperations extends PersistentStateHolder {


    public Row loadEntity();

    public Row loadProperty(PropertyMeta pm);

    public void pushInsertStatement();

    public void pushUpdateStatement(List<PropertyMeta> pms);

    public void pushCollectionAndMapUpdateStatements(DirtyCheckChangeSet changeSet);

    public void bindForDeletion(String tableName);

    // Simple counter
    public void bindForSimpleCounterIncrement(PropertyMeta counterMeta, Long increment);

    public Long getSimpleCounter(PropertyMeta counterMeta, ConsistencyLevel consistency);

    public void bindForSimpleCounterDeletion(PropertyMeta counterMeta);

    // Clustered counter
    public void pushClusteredCounterIncrementStatement(PropertyMeta counterMeta, Long increment);

    public Row getClusteredCounter();

    public Long getClusteredCounterColumn(PropertyMeta counterMeta);

    public void bindForClusteredCounterDeletion();

}
