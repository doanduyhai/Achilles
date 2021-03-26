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

package info.archinnov.achilles.internals.metamodel;

import static info.archinnov.achilles.internals.schema.SchemaValidator.*;
import static info.archinnov.achilles.internals.statements.PreparedStatementGenerator.*;
import static info.archinnov.achilles.validation.Validator.validateNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;

import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.injectable.*;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.BeanValueExtractor;
import info.archinnov.achilles.internals.schema.SchemaContext;
import info.archinnov.achilles.internals.schema.SchemaCreator;
import info.archinnov.achilles.internals.statements.BoundValuesWrapper;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.internals.utils.CollectionsHelper;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.type.interceptor.Event;
import info.archinnov.achilles.type.interceptor.Interceptor;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.validation.Validator;


public abstract class AbstractEntityProperty<T> implements
        InjectBeanFactory, InjectKeyspace,
        InjectConsistency, InjectInsertStrategy,
        InjectUserAndTupleTypeFactory,
        InjectJacksonMapper, InjectSchemaStrategy,
        InjectRuntimeCodecs {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEntityProperty.class);

    public final Logger entityLogger;
    public final Class<T> entityClass;
    public final Optional<String> staticKeyspace;
    public final Optional<String> staticTableOrViewName;
    public final String derivedTableOrViewName;
    public final BiMap<String, String> fieldNameToCqlColumn;
    public final boolean counterTable;
    public final Optional<ConsistencyLevel> staticReadConsistency;
    public final Optional<ConsistencyLevel> staticWriteConsistency;
    public final Optional<ConsistencyLevel> staticSerialConsistency;
    public final Optional<Integer> staticTTL;
    public final Optional<InsertStrategy> staticInsertStrategy;
    public final Optional<InternalNamingStrategy> staticNamingStrategy;
    public final List<AbstractProperty<T, ?, ?>> partitionKeys;
    public final List<AbstractProperty<T, ?, ?>> clusteringColumns;
    public final List<AbstractProperty<T, ?, ?>> staticColumns;
    public final List<AbstractProperty<T, ?, ?>> normalColumns;
    public final List<AbstractProperty<T, ?, ?>> computedColumns;
    public final List<AbstractProperty<T, ?, ?>> counterColumns;
    public final List<AbstractProperty<T, ?, ?>> constructorInjectedColumns;
    public final List<AbstractProperty<T, ?, ?>> allColumns;
    public final List<AbstractProperty<T, ?, ?>> allColumnsWithComputed;
    public final List<Interceptor<T>> interceptors = new ArrayList<>();
    protected BeanFactory beanFactory;
    protected Optional<String> keyspace = Optional.empty();
    protected ConsistencyLevel readConsistencyLevel;
    protected ConsistencyLevel writeConsistencyLevel;
    protected ConsistencyLevel serialConsistencyLevel;
    protected InsertStrategy insertStrategy;
    public Optional<SchemaNameProvider> schemaStrategy = Optional.empty();


    public AbstractEntityProperty() {
        entityClass = getEntityClass();
        entityLogger = LoggerFactory.getLogger(entityClass);
        staticKeyspace = getStaticKeyspace();
        staticTableOrViewName = getStaticTableOrViewName();
        derivedTableOrViewName = getDerivedTableOrViewName();
        fieldNameToCqlColumn = fieldNameToCqlColumn();
        counterTable = isCounterTable();
        staticReadConsistency = getStaticReadConsistency();
        staticWriteConsistency = getStaticWriteConsistency();
        staticSerialConsistency = getStaticSerialConsistency();
        staticTTL = getStaticTTL();
        staticInsertStrategy = getStaticInsertStrategy();
        staticNamingStrategy = getStaticNamingStrategy();
        partitionKeys = getPartitionKeys();
        clusteringColumns = getClusteringColumns();
        staticColumns = getStaticColumns();
        normalColumns = getNormalColumns();
        computedColumns = getComputedColumns();
        constructorInjectedColumns = getConstructorInjectedColumns();
        counterColumns = getCounterColumns();
        allColumns = getAllColumns();
        allColumnsWithComputed = getAllColumnsWithComputed();
    }

    protected abstract Class<T> getEntityClass();

    protected abstract Optional<String> getStaticKeyspace();

    protected abstract Optional<String> getStaticTableOrViewName();

    protected abstract String getDerivedTableOrViewName();

    protected abstract BiMap<String, String> fieldNameToCqlColumn();

    protected abstract boolean isCounterTable();

    protected abstract Optional<ConsistencyLevel> getStaticReadConsistency();

    protected abstract Optional<ConsistencyLevel> getStaticWriteConsistency();

    protected abstract Optional<ConsistencyLevel> getStaticSerialConsistency();

    protected abstract Optional<Integer> getStaticTTL();

    protected abstract Optional<InsertStrategy> getStaticInsertStrategy();

    protected abstract Optional<InternalNamingStrategy> getStaticNamingStrategy();

    protected abstract List<AbstractProperty<T, ?, ?>> getPartitionKeys();

    protected abstract List<AbstractProperty<T, ?, ?>> getClusteringColumns();

    protected abstract List<AbstractProperty<T, ?, ?>> getStaticColumns();

    protected abstract List<AbstractProperty<T, ?, ?>> getNormalColumns();

    protected abstract List<AbstractProperty<T, ?, ?>> getComputedColumns();

    protected abstract List<AbstractProperty<T, ?, ?>> getCounterColumns();

    protected abstract List<AbstractProperty<T, ?, ?>> getConstructorInjectedColumns();

    protected EntityType getType() {
        return EntityType.TABLE;
    }

    public ConsistencyLevel readConsistency(Optional<ConsistencyLevel> runtimeConsistency) {
        final ConsistencyLevel consistencyLevel = OverridingOptional
                .from(runtimeConsistency)
                .defaultValue(readConsistencyLevel)
                .get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determining runtime read consistency level for entity %s : %s",
                    entityClass.getCanonicalName(), consistencyLevel.name()));
        }
        return consistencyLevel;
    }

    public ConsistencyLevel writeConsistency(Optional<ConsistencyLevel> runtimeConsistency) {
        final ConsistencyLevel consistencyLevel = OverridingOptional
                .from(runtimeConsistency)
                .defaultValue(writeConsistencyLevel)
                .get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determining runtime write consistency level for entity %s : %s",
                    entityClass.getCanonicalName(), consistencyLevel.name()));
        }
        return consistencyLevel;
    }

    public ConsistencyLevel serialConsistency(Optional<ConsistencyLevel> runtimeConsistency) {
        final ConsistencyLevel consistencyLevel = OverridingOptional
                .from(runtimeConsistency)
                .defaultValue(serialConsistencyLevel)
                .get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determining runtime serial consistency level for entity %s : %s",
                    entityClass.getCanonicalName(), consistencyLevel.name()));
        }
        return consistencyLevel;
    }

    public boolean isCounter() {
        return counterColumns.size() > 0;
    }

    public boolean isClustered() {
        return clusteringColumns.size() > 0;
    }

    public boolean hasStaticColumn() {
        return staticColumns.size() > 0;
    }

    public InsertStrategy insertStrategy() {
        return staticInsertStrategy.orElse(insertStrategy);
    }

    public void triggerInterceptorsForEvent(Event event, T instance) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Trigger interceptors for entity %s on event %s",
                    instance, event.name()));
        }
        interceptors
                .stream()
                .filter(x -> x.interceptOnEvents().contains(event))
                .forEach(x -> x.onEvent(instance, event));
    }

    protected abstract T newInstanceFromCustomConstructor(Row row, List<String> cqlColumns);

    public T createEntityFrom(Row row) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Create entity of type %s from Cassandra row %s",
                    entityClass.getCanonicalName(), row));
        }
        if (row != null) {
            // No custom constructor
            final List<String> cqlColumns = row.getColumnDefinitions().asList().stream().map(def -> def.getName()).collect(toList());
            if (constructorInjectedColumns.size() == 0) {
                T newInstance = beanFactory.newInstance(entityClass);
                allColumnsWithComputed
                        .stream()
                        .filter(x -> cqlColumns.contains(x.getColumnForSelect()))
                        .forEach(x -> x.decodeField(row, newInstance));
                return newInstance;
            } else {

                final T newInstance = newInstanceFromCustomConstructor(row, cqlColumns);

                // Call setters for remaining fields not injected by constructor
                allColumnsWithComputed
                        .stream()
                        .filter(x -> !constructorInjectedColumns.contains(x))
                        .forEach(x -> x.decodeField(row, newInstance));
                return newInstance;
            }
        }
        return null;
    }

    public BoundValuesWrapper extractAllValuesFromEntity(T instance, CassandraOptions cassandraOptions) {
        return BeanValueExtractor.extractAllValues(instance, this, cassandraOptions);
    }

    public BoundValuesWrapper extractPartitionKeysAndStaticColumnsFromEntity(T instance, CassandraOptions cassandraOptions) {
        return BeanValueExtractor.extractPartitionKeysAndStaticValues(instance, this, cassandraOptions);
    }

    public Optional<String> getKeyspace() {
        final Optional<String> keyspace = OverridingOptional
                .from(staticNamingStrategy.flatMap(naming -> staticKeyspace.map(ks -> naming.apply(ks))))
                .andThen(staticKeyspace)
                .andThen(schemaStrategy.map(x -> x.keyspaceFor(entityClass)))
                .andThen(this.keyspace)
                .getOptional();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determine runtime keyspace for entity of type %s : %s",
                    entityClass.getCanonicalName(), keyspace));
        }
        return keyspace;
    }

    public String getTableOrViewName() {
        final String tableName = OverridingOptional
                .from(staticNamingStrategy.flatMap(naming -> staticTableOrViewName.map(ks -> naming.apply(ks))))
                .andThen(staticTableOrViewName)
                .andThen(schemaStrategy.map(x -> x.tableNameFor(entityClass)))
                .defaultValue(derivedTableOrViewName)
                .get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determine runtime table name for entity of type %s : %s",
                    entityClass.getCanonicalName(), tableName));
        }
        return tableName;
    }

    public void prepareStaticStatements(InternalCassandraVersion cassandraVersion, Session session, StatementsCache cache) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Preparing static statements for entity of type %s",
                    entityClass.getCanonicalName()));
        }
        if (!counterTable) {
            generateStaticInsertQueries(cassandraVersion, session, cache, this);
        }

        generateStaticDeleteQueries(session, cache, this);
        generateStaticSelectQuery(session, cache, this);
    }

    protected List<AbstractProperty<T, ?, ?>> getAllColumns() {
        return CollectionsHelper.appendAll(partitionKeys, staticColumns,
                clusteringColumns, normalColumns, counterColumns);
    }

    protected List<AbstractProperty<T, ?, ?>> getAllColumnsWithComputed() {
        return CollectionsHelper.appendAll(partitionKeys, staticColumns,
                clusteringColumns, normalColumns, counterColumns, computedColumns);
    }

    public String generateSchema(SchemaContext context) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating DDL script for entity of type %s",
                    entityClass.getCanonicalName()));
        }
        StringJoiner joiner = new StringJoiner("\n\n");
        SchemaCreator.generateTable_And_Indices(context, this)
                .forEach(joiner::add);
        return joiner.toString();
    }

    public void validateSchema(ConfigurationContext configContext) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Validating schema for entity of type %s",
                    entityClass.getCanonicalName()));
        }
        final String keyspace = getKeyspace().orElse(null);

        Validator.validateNotBlank(keyspace,
                "Current keyspace not provided neither in configuration nor on entity '%s' annotation", entityClass.getCanonicalName());

        final KeyspaceMetadata keyspaceMetadata = configContext
                .getSession()
                .getCluster()
                .getMetadata()
                .getKeyspace(keyspace);

        validateNotNull(keyspaceMetadata,"The keyspace %s defined on entity %s does not exist in Cassandra",
                keyspace, entityClass.getCanonicalName());

        final String tableName = getTableOrViewName();

        final TableMetadata tableMetadata = keyspaceMetadata
                .getTable(tableName);

        validateNotNull(tableMetadata,"The table %s defined on entity %s does not exist in Cassandra",
                tableName, entityClass.getCanonicalName());

        validateColumnType(ColumnType.PARTITION, tableMetadata, partitionKeys, entityClass);
        validateColumnType(ColumnType.CLUSTERING, tableMetadata, clusteringColumns, entityClass);
        validateColumnType(ColumnType.STATIC, tableMetadata, staticColumns, entityClass);

        validateDefaultTTL(tableMetadata, staticTTL, entityClass);
        validateColumns(tableMetadata, partitionKeys, entityClass);
        validateColumns(tableMetadata, clusteringColumns, entityClass);
        validateColumns(tableMetadata, staticColumns, entityClass);
        validateColumns(tableMetadata, normalColumns, entityClass);
        validateColumns(tableMetadata, counterColumns, entityClass);
    }

    @Override
    public void injectKeyspace(String keyspace) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting global keyspace name %s into entity meta of %s",
                    keyspace, entityClass.getCanonicalName()));
        }
        this.keyspace = Optional.of(keyspace);

        allColumns.stream().forEach(x -> x.injectKeyspace(keyspace));
    }

    @Override
    public void injectConsistencyLevels(Session session, ConfigurationContext configContext) {

        /**
         * Consistency priority ordering (from highest to lowest)
         * 1. Set at runtime (see this.writeConsistency(), this.readConsistency() and this.serialConsistency() )
         * 2. Defined by static annotation on entity
         * 3. Defined by Consistency Level Map as Achilles Config
         * 4. Defined by Achilles Config
         * 5. Defined as QueryOptions in the injected Cluster object
         * 6. Hard-coded value LOCAL_ONE & LOCAL_SERIAL
         */

        ConsistencyLevel clusterConsistency = session.getCluster().getConfiguration().getQueryOptions().getConsistencyLevel();
        ConsistencyLevel clusterSerialConsistency = session.getCluster().getConfiguration().getQueryOptions().getSerialConsistencyLevel();

        final String tableOrViewName = this.getTableOrViewName();
        this.readConsistencyLevel =
                OverridingOptional.from(staticReadConsistency)
                        .andThen(configContext.getReadConsistencyLevelForTable(tableOrViewName))
                        .andThen(configContext.getDefaultReadConsistencyLevel())
                        .andThen(clusterConsistency)
                        .defaultValue(ConfigurationContext.DEFAULT_CONSISTENCY_LEVEL)
                        .get();

        this.writeConsistencyLevel =
                OverridingOptional.from(staticWriteConsistency)
                        .andThen(configContext.getWriteConsistencyLevelForTable(tableOrViewName))
                        .andThen(configContext.getDefaultWriteConsistencyLevel())
                        .andThen(clusterConsistency)
                        .defaultValue(ConfigurationContext.DEFAULT_CONSISTENCY_LEVEL)
                        .get();


        this.serialConsistencyLevel =
                OverridingOptional.from(staticSerialConsistency)
                        .andThen(configContext.getSerialConsistencyLevelForTable(tableOrViewName))
                        .andThen(clusterSerialConsistency)
                        .andThen(configContext.getDefaultSerialConsistencyLevel())
                        .defaultValue(ConfigurationContext.DEFAULT_SERIAL_CONSISTENCY_LEVEL)
                        .get();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting read/write/serial consistency levels %s/%s/%s into entity meta of %s",
                    readConsistencyLevel.name(),writeConsistencyLevel.name(),serialConsistencyLevel.name(),
                    entityClass.getCanonicalName()));
        }
    }

    @Override
    public void inject(InsertStrategy insertStrategy) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting insert strategy %s into entity meta of %s",
                    insertStrategy, entityClass.getCanonicalName()));
        }
        if (!staticInsertStrategy.isPresent()) this.insertStrategy = insertStrategy;
        else this.insertStrategy = staticInsertStrategy.get();
    }

    @Override
    public void inject(SchemaNameProvider schemaNameProvider) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting global schema name provider %s into entity meta of %s",
                    schemaNameProvider, entityClass.getCanonicalName()));
        }
        this.schemaStrategy = Optional.ofNullable(schemaNameProvider);
        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.inject(schemaNameProvider);
        }
    }

    @Override
    public void inject(BeanFactory factory) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting bean factory %s into entity meta of %s",
                    factory, entityClass.getCanonicalName()));
        }
        beanFactory = factory;

        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.inject(factory);
        }

    }

    @Override
    public void inject(UserTypeFactory userTypeFactory, TupleTypeFactory tupleTypeFactory) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting user type factory %s and tuple type factory %s into entity meta of %s",
                    userTypeFactory, tupleTypeFactory, entityClass.getCanonicalName()));
        }

        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.inject(userTypeFactory, tupleTypeFactory);
        }
    }

    @Override
    public void inject(ObjectMapper jacksonMapper) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting Jackson Object Mapper instance %s into entity meta of %s",
                    jacksonMapper, entityClass.getCanonicalName()));
        }

        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.inject(jacksonMapper);
        }
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting runtime codecs into entity meta of %s",
                    entityClass.getCanonicalName()));
        }

        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.injectRuntimeCodecs(runtimeCodecs);
        }
    }

    public boolean isTable() {
        return true;
    }

    public boolean isView() {
        return false;
    }

    public enum EntityType {
        TABLE, VIEW
    }
}
