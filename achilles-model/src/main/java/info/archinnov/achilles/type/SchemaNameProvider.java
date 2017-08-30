/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

package info.archinnov.achilles.type;

/**
 * Interface to implement if you want to provide your own
 * schema name provider. This feature is useful for multi-tenant applications
 */
public interface SchemaNameProvider {

    /**
     * Provide keyspace name for entity class
     */
    <T> String keyspaceFor(Class<T> entityClass);

    /**
     * Provide table name for entity class
     */
    <T> String tableNameFor(Class<T> entityClass);
}
