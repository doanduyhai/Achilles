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

package info.archinnov.achilles.internals.metamodel;

import static info.archinnov.achilles.internals.schema.SchemaValidator.validateColumns;
import static info.archinnov.achilles.internals.schema.SchemaValidator.validateDefaultTTL;
import static info.archinnov.achilles.internals.statements.PreparedStatementGenerator.*;
import static info.archinnov.achilles.validation.Validator.validateNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;

import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.injectable.*;
import info.archinnov.achilles.internals.options.Options;
import info.archinnov.achilles.internals.runtime.BeanValueExtractor;
import info.archinnov.achilles.internals.schema.SchemaContext;
import info.archinnov.achilles.internals.schema.SchemaCreator;
import info.archinnov.achilles.internals.statements.BoundValuesWrapper;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.type.interceptor.Event;
import info.archinnov.achilles.type.interceptor.Interceptor;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.tuples.Tuple3;
import info.archinnov.achilles.validation.Validator;


public abstract class AbstractEntityProperty<T> implements
        InjectBeanFactory, InjectKeyspace,
        InjectConsistency, InjectInsertStrategy,
        InjectTupleTypeFactory, InjectUserTypeFactory,
        InjectJacksonMapper, InjectSchemaStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEntityProperty.class);

    public final Logger entityLogger;
    public final Class<T> entityClass;
    public final Optional<String> staticKeyspace;
    public final Optional<String> staticTableName;
    public final String derivedTableName;
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
    public final List<AbstractProperty<T, ?, ?>> allColumns;
    public final List<AbstractProperty<T, ?, ?>> allColumnsWithComputed;
    public final List<Interceptor<T>> interceptors = new ArrayList<>();
    protected BeanFactory beanFactory;
    protected Optional<String> keyspace = Optional.empty();
    protected ConsistencyLevel readConsistencyLevel;
    protected ConsistencyLevel writeConsistencyLevel;
    protected ConsistencyLevel serialConsistencyLevel;
    protected InsertStrategy insertStrategy;
    protected Optional<SchemaNameProvider> schemaStrategy = Optional.empty();


    public AbstractEntityProperty() {
        entityClass = getEntityClass();
        entityLogger = LoggerFactory.getLogger(entityClass);
        staticKeyspace = getStaticKeyspace();
        staticTableName = getStaticTableName();
        derivedTableName = getDerivedTableName();
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
        counterColumns = getCounterColumns();
        allColumns = getAllColumns();
        allColumnsWithComputed = getAllColumnsWithComputed();
    }

    protected abstract Class<T> getEntityClass();

    protected abstract Optional<String> getStaticKeyspace();

    protected abstract Optional<String> getStaticTableName();

    protected abstract String getDerivedTableName();

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

    public ConsistencyLevel readConsistency(Optional<ConsistencyLevel> runtimeConsistency) {
        final ConsistencyLevel consistencyLevel = OverridingOptional
                .from(runtimeConsistency)
                .andThen(staticReadConsistency)
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
                .andThen(staticWriteConsistency)
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
                .andThen(staticSerialConsistency)
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

    public T createEntityFrom(Row row) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Create entity of type %s from Cassandra row %s",
                    entityClass.getCanonicalName(), row));
        }
        if (row != null) {
            T newInstance = beanFactory.newInstance(entityClass);
            final List<String> cqlColumns = row.getColumnDefinitions().asList().stream().map(def -> def.getName()).collect(toList());
            allColumnsWithComputed
                    .stream()
                    .filter(x -> cqlColumns.contains(x.getColumnForSelect()))
                    .forEach(x -> x.decodeField(row, newInstance));
            return newInstance;
        }
        return null;
    }

    public BoundValuesWrapper extractAllValuesFromEntity(T instance, Options options) {
        return BeanValueExtractor.extractAllValues(instance, this, options);
    }

    public Optional<String> getKeyspace() {
        final Optional<String> keyspace = OverridingOptional
                .from(staticKeyspace)
                .andThen(schemaStrategy.map(x -> x.keyspaceFor(entityClass)))
                .andThen(this.keyspace)
                .getOptional();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determine runtime keyspace for entity of type %s : %s",
                    entityClass.getCanonicalName(), keyspace));
        }
        return keyspace;
    }

    public String getTableName() {
        final String tableName = OverridingOptional
                .from(staticTableName)
                .andThen(schemaStrategy.map(x -> x.tableNameFor(entityClass)))
                .defaultValue(derivedTableName)
                .get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Determine runtime table name for entity of type %s : %s",
                    entityClass.getCanonicalName(), tableName));
        }
        return tableName;
    }

    public void prepareStaticStatements(Session session, StatementsCache cache) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Preparing static statements for entity of type %s",
                    entityClass.getCanonicalName()));
        }
        if (!counterTable) {
            generateStaticInsertQueries(session, cache, this);
        }
        generateStaticDeleteQueries(session, cache, this);
        generateStaticSelectQuery(session, cache, this);
    }

    protected List<AbstractProperty<T, ?, ?>> getAllColumns() {
        List<AbstractProperty<T, ?, ?>> allColumns = new ArrayList<>();
        allColumns.addAll(partitionKeys);
        allColumns.addAll(staticColumns);
        allColumns.addAll(clusteringColumns);
        allColumns.addAll(normalColumns);
        allColumns.addAll(counterColumns);
        return allColumns;
    }

    protected List<AbstractProperty<T, ?, ?>> getAllColumnsWithComputed() {
        List<AbstractProperty<T, ?, ?>> allColumns = new ArrayList<>();
        allColumns.addAll(partitionKeys);
        allColumns.addAll(staticColumns);
        allColumns.addAll(clusteringColumns);
        allColumns.addAll(normalColumns);
        allColumns.addAll(counterColumns);
        allColumns.addAll(computedColumns);
        return allColumns;
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

        validateNotNull(keyspaceMetadata,"The keyspace {} defined on entity {} does not exist in Cassandra",
                keyspace, entityClass.getCanonicalName());

        final String tableName = getTableName();

        final TableMetadata tableMetadata = keyspaceMetadata
                .getTable(tableName);

        validateNotNull(tableMetadata,"The table {} defined on entity {} does not exist in Cassandra",
                tableName, entityClass.getCanonicalName());

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
    }

    @Override
    public void inject(Tuple3<ConsistencyLevel, ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting read/write/serial consistency levels %s into entity meta of %s",
                    consistencyLevels, entityClass.getCanonicalName()));
        }
        this.readConsistencyLevel = consistencyLevels._1();
        this.writeConsistencyLevel = consistencyLevels._2();
        this.serialConsistencyLevel = consistencyLevels._3();
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
    public void inject(TupleTypeFactory factory) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting tuple type factory %s into entity meta of %s",
                    factory, entityClass.getCanonicalName()));
        }

        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.inject(factory);
        }
    }

    @Override
    public void inject(UserTypeFactory factory) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Injecting user type factory %s into entity meta of %s",
                    factory, entityClass.getCanonicalName()));
        }

        for (AbstractProperty<T, ?, ?> x : allColumns) {
            x.inject(factory);
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
}
