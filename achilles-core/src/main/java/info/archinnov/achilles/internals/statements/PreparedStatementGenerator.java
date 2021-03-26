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

package info.archinnov.achilles.internals.statements;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.internals.cache.CacheKey.Operation.*;
import static java.lang.String.format;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.*;

import info.archinnov.achilles.internals.cache.CacheKey;
import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.cassandra_version.CassandraFeature;
import info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.ComputedProperty;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.validation.Validator;

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

        for (AbstractProperty<?, ?, ?> x : entityProperty.allColumns) {
            select.column(x.fieldInfo.quotedCqlColumn);
        }

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
                from = select.from(keyspace.get(), entityProperty.getTableOrViewName());
            } else {
                from = select.from(entityProperty.getTableOrViewName());
            }
        }

        final Select.Where where = from.where();

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            where.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.clusteringColumns) {
            where.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        return where;
    }

    public static void generateStaticDeleteQueries(Session session, StatementsCache cache,  AbstractEntityProperty<?> entityProperty) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE queries for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

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
        from = lookupSchemaFromProvider(entityProperty, schemaNameProvider, delete, keyspace);

        final Delete.Where deleteByKeys = from.where();

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            deleteByKeys.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.clusteringColumns) {
            deleteByKeys.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        return deleteByKeys;
    }

    public static RegularStatement generateDeleteByKeysIfExists( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE IF EXISTS query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Delete.Selection delete = QueryBuilder.delete();
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Delete from;
        from = lookupSchemaFromProvider(entityProperty, schemaNameProvider, delete, keyspace);

        final Delete.Where deleteByKeysIfExists = from.ifExists().where();

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            deleteByKeysIfExists.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.clusteringColumns) {
            deleteByKeysIfExists.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        return deleteByKeysIfExists;
    }



    public static RegularStatement generateDeleteByPartition( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate DELETE BY PARTITION query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Delete.Selection delete = QueryBuilder.delete();
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Delete from;
        from = lookupSchemaFromProvider(entityProperty, schemaNameProvider, delete, keyspace);

        final Delete.Where deleteByPartition = from.where();

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            deleteByPartition.and(eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn)));
        }

        return deleteByPartition;
    }


    public static void generateStaticInsertQueries(InternalCassandraVersion cassandraVersion, Session session, StatementsCache cache, AbstractEntityProperty<?> entityProperty) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT queries for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT),
                () -> session.prepare(generateInsert(entityProperty, Optional.empty())));

        cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT_IF_NOT_EXISTS),
                () -> session.prepare(generateInsertIfNotExists(entityProperty, Optional.empty())));

        if (cassandraVersion.supportsFeature(CassandraFeature.JSON)) {
            cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT_JSON),
                    () -> session.prepare(generateInsertJSON(entityProperty, Optional.empty())));

            cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT_IF_NOT_EXISTS_JSON),
                    () -> session.prepare(generateInsertIfNotExistsJson(entityProperty, Optional.empty())));
        }

        if (entityProperty.hasStaticColumn()) {
            cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT_STATIC),
                    () -> session.prepare(generateInsertStatic(entityProperty, Optional.empty())));

            cache.putStaticCache(new CacheKey(entityProperty.entityClass, INSERT_STATIC_IF_NOT_EXISTS),
                    () -> session.prepare(generateInsertStaticIfNotExists(entityProperty, Optional.empty())));
        }
    }

    public static RegularStatement generateInsert( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);

        for (AbstractProperty<?, ?, ?> x : entityProperty.allColumns) {
            insert.value(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn));
        }

        return insert.using(ttl(bindMarker("ttl")));
    }

    public static <T> RegularStatement generateUpdate(T instance, AbstractEntityProperty<T> entityProperty, CassandraOptions options,
                                                      boolean staticValuesOnly, boolean ifExists) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate UPDATE query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Update update = getUpdateWithTableName(entityProperty, options.getSchemaNameProvider());

        if (options.hasDefaultTimestamp()) {
            update.using(QueryBuilder.timestamp(bindMarker("timestamp")));
        }
        update.using(QueryBuilder.ttl(bindMarker("ttl")));

        Update.Assignments assignments = update.with();

        entityProperty
                .allColumns
                .stream()
                .filter(x -> x.fieldInfo.columnType != ColumnType.PARTITION)
                .filter(x -> x.fieldInfo.columnType != ColumnType.CLUSTERING)
                .filter(x -> staticValuesOnly ? x.fieldInfo.columnType == ColumnType.STATIC : true)
                .filter(x -> x.getJavaValue(instance) != null)
                .forEach(x -> assignments.and(QueryBuilder.set(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn))));

        final Update.Where where = update.where();
        entityProperty
                .partitionKeys
                .forEach(x -> where.and(QueryBuilder.eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn))));

        if (!staticValuesOnly) {
            entityProperty
                    .clusteringColumns
                    .forEach(x -> where.and(QueryBuilder.eq(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn))));
        }

        if (ifExists) {
            where.ifExists();
        }

        return where;
    }

    public static RegularStatement generateInsertJSON(AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT JSON query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);

        insert.json(QueryBuilder.bindMarker("json"));

        return insert.using(ttl(bindMarker("ttl")));
    }

    public static RegularStatement generateInsertStatic( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT STATIC query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        Validator.validateBeanMappingTrue(entityProperty.hasStaticColumn(),
                "Cannot generate INSERT STATIC query for entity of type %s because it has no static column");

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            insert.value(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.staticColumns) {
            insert.value(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn));
        }

        return insert.using(ttl(bindMarker("ttl")));
    }

    public static RegularStatement generateInsertIfNotExists( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT IF NOT EXISTS query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);

        for (AbstractProperty<?, ?, ?> x : entityProperty.allColumns) {
            insert.value(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn));
        }

        return insert.ifNotExists().using(ttl(bindMarker("ttl")));
    }

    public static RegularStatement generateInsertIfNotExistsJson( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT JSON ... IF NOT EXISTS query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);

        insert.json(QueryBuilder.bindMarker("json"));

        return insert.ifNotExists().using(ttl(bindMarker("ttl")));
    }

    public static RegularStatement generateInsertStaticIfNotExists( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generate INSERT STATIC IF NOT EXISTS query for entity of type %s", entityProperty.entityClass.getCanonicalName()));
        }

        Validator.validateBeanMappingTrue(entityProperty.hasStaticColumn(),
                "Cannot generate INSERT IF NOT EXISTS query for entity of type %s because it has no static column");

        final Insert insert = getInsertWithTableName(entityProperty, schemaNameProvider);

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            insert.value(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.staticColumns) {
            insert.value(x.fieldInfo.quotedCqlColumn, bindMarker(x.fieldInfo.quotedCqlColumn));
        }

        return insert.ifNotExists().using(ttl(bindMarker("ttl")));
    }

    private static Insert getInsertWithTableName( AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Insert insert;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Get INSERT query for entity of type %s with schema provider %s",
                        entityProperty.entityClass.getCanonicalName(), provider));
            }
            insert = QueryBuilder.insertInto(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                insert = QueryBuilder.insertInto(keyspace.get(), entityProperty.getTableOrViewName());
            } else {
                insert = QueryBuilder.insertInto(entityProperty.getTableOrViewName());
            }
        }
        return insert;
    }

    private static Update getUpdateWithTableName(AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider) {
        final Optional<String> keyspace = entityProperty.getKeyspace();
        final Update update;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Get UPDATE query for entity of type %s with schema provider %s",
                        entityProperty.entityClass.getCanonicalName(), provider));
            }
            update = QueryBuilder.update(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                update = QueryBuilder.update(keyspace.get(), entityProperty.getTableOrViewName());
            } else {
                update = QueryBuilder.update(entityProperty.getTableOrViewName());
            }
        }
        return update;
    }

    private static Delete lookupSchemaFromProvider(AbstractEntityProperty<?> entityProperty, Optional<SchemaNameProvider> schemaNameProvider, Delete.Selection delete, Optional<String> keyspace) {
        Delete from;
        if (schemaNameProvider.isPresent()) {
            final SchemaNameProvider provider = schemaNameProvider.get();
            from = delete.from(provider.keyspaceFor(entityProperty.entityClass), provider.tableNameFor(entityProperty.entityClass));
        } else {
            if (keyspace.isPresent()) {
                from = delete.from(keyspace.get(), entityProperty.getTableOrViewName());
            } else {
                from = delete.from(entityProperty.getTableOrViewName());
            }
        }
        return from;
    }
}
