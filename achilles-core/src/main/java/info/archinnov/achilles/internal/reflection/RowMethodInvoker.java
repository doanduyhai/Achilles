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
package info.archinnov.achilles.internal.reflection;

import static info.archinnov.achilles.internal.cql.TypeMapper.getRowMethod;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCompatibleJavaType;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.internal.metadata.holder.PropertyMetaRowExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

public class RowMethodInvoker {
    private static final Logger log = LoggerFactory.getLogger(RowMethodInvoker.class);

    @SuppressWarnings("unchecked")
    public <T> T invokeOnRowForType(Row row, Class<T> type, String name) {
        log.trace("Extract property {} of type {} from CQL row ", name, type);
        try {
            return (T) getRowMethod(type).invoke(row, name);
        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve column '" + name + "' of type '" + type.getCanonicalName()
                    + "' from CQL Row", e);
        }
    }
}
