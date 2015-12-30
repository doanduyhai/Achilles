/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.statements;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.internals.cache.CacheKey.Operation.*;
import static java.lang.String.format;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import info.archinnov.achilles.internals.cache.CacheKey;
import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.ComputedProperty;
import info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo;
import info.archinnov.achilles.type.SchemaNameProvider;

public class PreparedStatementGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementGenerator.class);

    public static void generateStaticSelectQuery(Session session, StatementsCache cache,  AbstractEntityProperty<?> entityProperty) {
        final RegularStatement where = generateSelectQuery(entityProperty, Optional.empty());
        cache.putStaticCache(new CacheKey(entityProperty.entityClass, FIND), () -> session.prepare(where));
    }

    public static RegularStatement generateSelectQuery( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate SELECT query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Select.Selection select = QueryBuilder.select();
        final Optional<String> keyspace = entityProperty.getKeyspace();

        entityProperty
                .allColumns
                .forEach(x -> select.column(x.fieldInfo.cqlColumn));

        entityProperty
                .computedColumns
                .stream()
                .map(x -> (ComputedProperty) x)
                .forEach(x -> {
                    final ComputedColumnInfo columnInfo = x.computedColumnInfo;
                    final Object[] args = columnInfo.functionArgs
                            .stream()
                            .map(QueryBuilder::column)
                            .toArray();

                    select.fcall(columnInfo.functionName, args).as(columnInfo.alias);
                });

        final Select from;

        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            from = select.from(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                from = select.from(keyspace.get(), entityProperty.getTableName());
            } else {
                from = select.from(entityProperty.getTableName());
            }
        }

        final Select.Where where = from.where();

        entityProperty.partitionKeys.forEach(x -> where.and(eq(x.fieldInfo.cqlColumn, bindMarker(x.fieldInfo.cqlColumn))));
        entityProperty.clusteringColumns.forEach(x -> where.and(eq(x.fieldInfo.cqlColumn, bindMarker(x.fieldInfo.cqlColumn))));
        return where;
    }

    public static void generateStaticDeleteQueries(Session session, StatementsCache cache,  AbstractEntityProperty<?> entityProperty) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE queries for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Delete.Selection delete = QueryBuilder.delete();
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Delete from;
        if (keyspace.isPresent()) {
            from = delete.from(keyspace.get(), entityProperty.getTableName());
        } else {
            from = delete.from(entityProperty.getTableName());
        }

        final Delete.Where deleteByKeys = from.where();
        final Delete.Where deleteByKeysIfExists = from.ifExists().where();
        final Delete.Where deleteByPartition = from.where();

        entityProperty.partitionKeys
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByKeys.and(eq(cqlColumn, bindMarker(cqlColumn)));
                    deleteByKeysIfExists.and(eq(cqlColumn, bindMarker(cqlColumn)));
                    deleteByPartition.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });

        entityProperty.clusteringColumns
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByKeys.and(eq(cqlColumn, bindMarker(cqlColumn)));
                    deleteByKeysIfExists.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });


        cache.putStaticCache(new CacheKey(entityProperty.entityClass, DELETE),
                () -> session.prepare(generateDeleteByKeys(entityProperty, Optional.empty())));

        if (!entityProperty.isCounter()) {
            cache.putStaticCache(new CacheKey(entityProperty.entityClass, DELETE_IF_EXISTS),
                    () -> session.prepare(generateDeleteByKeysIfExists(entityProperty, Optional.empty())));
        }

        if (entityProperty.isClustered()) {
            cache.putStaticCache(new CacheKey(entityProperty.entityClass, DELETE_BY_PARTITION),
                    () -> session.prepare(generateDeleteByPartition(entityProperty, Optional.empty())));
        }
    }

    public static RegularStatement generateDeleteByKeys( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Delete.Selection delete = QueryBuilder.delete();
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Delete from;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            from = delete.from(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                from = delete.from(keyspace.get(), entityProperty.getTableName());
            } else {
                from = delete.from(entityProperty.getTableName());
            }
        }

        final Delete.Where deleteByKeys = from.where();

        entityProperty.partitionKeys
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByKeys.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });

        entityProperty.clusteringColumns
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByKeys.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });

        return deleteByKeys;
    }

    public static RegularStatement generateDeleteByKeysIfExists( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE IF EXISTS query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Delete.Selection delete = QueryBuilder.delete();
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Delete from;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            from = delete.from(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                from = delete.from(keyspace.get(), entityProperty.getTableName());
            } else {
                from = delete.from(entityProperty.getTableName());
            }
        }

        final Delete.Where deleteByKeysIfExists = from.ifExists().where();

        entityProperty.partitionKeys
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByKeysIfExists.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });

        entityProperty.clusteringColumns
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByKeysIfExists.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });

        return deleteByKeysIfExists;
    }

    public static RegularStatement generateDeleteByPartition( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE BY PARTITION query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Delete.Selection delete = QueryBuilder.delete();
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Delete from;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            from = delete.from(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                from = delete.from(keyspace.get(), entityProperty.getTableName());
            } else {
                from = delete.from(entityProperty.getTableName());
            }
        }

        final Delete.Where deleteByPartition = from.where();

        entityProperty.partitionKeys
                .forEach(x -> {
                    final String cqlColumn = x.fieldInfo.cqlColumn;
                    deleteByPartition.and(eq(cqlColumn, bindMarker(cqlColumn)));
                });

        return deleteByPartition;
    }


    public static void generateStaticInsertQueries(Session session, StatementsCache cache,  AbstractEntityProperty<?> entityProperty) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT queries for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT),
                () -> session.prepare(generateInsert(entityProperty, Optional.empty())));

        cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT_IF_NOT_EXISTS),
                () -> session.prepare(generateInsertIfNotExists(entityProperty, Optional.empty())));
    }

    public static RegularStatement generateInsert( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);
        entityProperty.allColumns.forEach(x -> insert.value(x.fieldInfo.cqlColumn, bindMarker(x.fieldInfo.cqlColumn)));
        return insert.using(ttl(bindMarker("ttl")));
    }

    public static RegularStatement generateInsertIfNotExists( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT IF NOT EXISTS query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);
        entityProperty.allColumns.forEach(x -> insert.value(x.fieldInfo.cqlColumn, bindMarker(x.fieldInfo.cqlColumn)));
        return insert.ifNotExists().using(ttl(bindMarker("ttl")));
    }

    private static Insert getInsertWithTableName( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {


        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Insert insert;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Get INSERT query for entity of type %s with schema provider",
                        entityProperty.entityClass.getCanonicalName(), provider));
            }
            insert = QueryBuilder.insertInto(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                insert = QueryBuilder.insertInto(keyspace.get(), entityProperty.getTableName());
            } else {
                insert = QueryBuilder.insertInto(entityProperty.getTableName());
            }
        }
        return insert;
    }
}
