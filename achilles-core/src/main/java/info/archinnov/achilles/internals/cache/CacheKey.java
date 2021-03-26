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

package info.archinnov.achilles.internals.cache;

import static info.archinnov.achilles.internals.statements.PreparedStatementGenerator.*;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.type.SchemaNameProvider;

public class CacheKey {

    private final Class<?> entityClass;
    private final Operation operation;

    public CacheKey(Class<?> entityClass, Operation operation) {
        this.entityClass = entityClass;
        this.operation = operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(entityClass, cacheKey.entityClass) &&
                Objects.equals(operation, cacheKey.operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityClass, operation);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheKey{");
        sb.append("entityClass=").append(entityClass);
        sb.append(", operation=").append(operation);
        sb.append('}');
        return sb.toString();
    }

    public enum Operation {

        FIND {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare SELECT statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }

                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, FIND));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateSelectQuery(meta, provider))
                        .orElse(psFromCache);

            }
        },
        INSERT {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare INSERT statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, INSERT));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateInsert(meta, provider))
                        .orElse(psFromCache);
            }
        },
        INSERT_JSON {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare INSERT JSON statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, INSERT_JSON));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateInsertJSON(meta, provider))
                        .orElse(psFromCache);
            }
        },
        INSERT_STATIC {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare INSERT STATIC statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, INSERT_STATIC));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateInsertStatic(meta, provider))
                        .orElse(psFromCache);
            }
        },
        INSERT_IF_NOT_EXISTS {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare INSERT IF NOT EXISTS statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, INSERT_IF_NOT_EXISTS));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateInsertIfNotExists(meta, provider))
                        .orElse(psFromCache);
            }
        },
        INSERT_IF_NOT_EXISTS_JSON {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare INSERT JSON ... IF NOT EXISTS statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, INSERT_IF_NOT_EXISTS_JSON));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateInsertIfNotExistsJson(meta, provider))
                        .orElse(psFromCache);
            }
        },
        INSERT_STATIC_IF_NOT_EXISTS {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare INSERT STATIC IF NOT EXISTS statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, INSERT_STATIC_IF_NOT_EXISTS));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateInsertStaticIfNotExists(meta, provider))
                        .orElse(psFromCache);
            }
        },
        DELETE {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare DELETE statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, DELETE));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateDeleteByKeys(meta, provider))
                        .orElse(psFromCache);
            }
        },
        DELETE_IF_EXISTS {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare DELETE IF EXISTS statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, DELETE_IF_EXISTS));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateDeleteByKeysIfExists(meta, provider))
                        .orElse(psFromCache);
            }
        },
        DELETE_BY_PARTITION {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("Prepare DELETE BY PARTITION statement for entity of type %s",
                            meta.entityClass.getCanonicalName()));
                }
                final Optional<SchemaNameProvider> provider = cassandraOptions.getSchemaNameProvider();
                final PreparedStatement psFromCache = rte.getStaticCache(new CacheKey(meta.entityClass, DELETE_BY_PARTITION));
                return rte.maybePrepareIfDifferentSchemaNameFromCache(meta, psFromCache, provider, () -> generateDeleteByPartition(meta, provider))
                        .orElse(psFromCache);
            }
        },
        UPDATE {
            @Override
            public PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions) {
                //TODO
                return null;
            }
        };

        private static final Logger LOGGER = LoggerFactory.getLogger(Operation.class);

        public abstract PreparedStatement getPreparedStatement(RuntimeEngine rte, AbstractEntityProperty<?> meta, CassandraOptions cassandraOptions);
    }
}
