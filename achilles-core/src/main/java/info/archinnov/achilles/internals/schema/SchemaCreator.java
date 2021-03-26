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

package info.archinnov.achilles.internals.schema;

import static java.lang.String.format;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.AbstractUDTClassProperty;
import info.archinnov.achilles.internals.metamodel.AbstractViewProperty;
import info.archinnov.achilles.internals.metamodel.columns.ClusteringColumnInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.logger.AchillesLoggers;
import info.archinnov.achilles.type.tuples.Tuple2;

public class SchemaCreator {
    public static final Logger ACHILLES_DML_LOGGER = LoggerFactory.getLogger(AchillesLoggers.ACHILLES_DDL_SCRIPT);

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCreator.class);

    public static List<String> generateTable_And_Indices(SchemaContext context, AbstractEntityProperty<?> entityProperty) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating table, udt type and indices for entity of type %s",
                    entityProperty.entityClass.getCanonicalName()));
        }

        final List<String> schemas = new ArrayList<>();

        final StringBuilder builder = new StringBuilder();
        final Optional<String> keyspace = Optional.ofNullable(context.keyspace.orElse(entityProperty.staticKeyspace.orElse(null)));
        final Create table;
        final String tableName = entityProperty.getTableOrViewName();
        final Optional<String> staticKeyspace = entityProperty.getKeyspace();
        final Optional<String> overridenKeyspace = OverridingOptional
                .from(staticKeyspace)
                .andThen(keyspace)
                .getOptional();

        if (overridenKeyspace.isPresent()) {
            table = SchemaBuilder.createTable(overridenKeyspace.get(), tableName).ifNotExists();
        } else {
            table = SchemaBuilder.createTable(tableName).ifNotExists();
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.partitionKeys) {
            table.addPartitionKey(x.fieldInfo.quotedCqlColumn, x.buildType(Optional.empty()));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.clusteringColumns) {
            table.addClusteringColumn(x.fieldInfo.quotedCqlColumn, x.buildType(Optional.empty()));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.staticColumns) {
            table.addStaticColumn(x.fieldInfo.quotedCqlColumn, x.buildType(Optional.empty()));
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.normalColumns) {
            final DataType dataType = x.buildType(Optional.empty());

            if (!x.fieldInfo.columnInfo.frozen && (dataType instanceof UserType)) {
                final String udtLiteral = dataType.toString().replaceAll("frozen<(.+)>", "$1");
                table.addUDTColumn(x.fieldInfo.quotedCqlColumn, SchemaBuilder.udtLiteral(udtLiteral));
            } else {
                table.addColumn(x.fieldInfo.quotedCqlColumn, dataType);
            }
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.counterColumns) {
            table.addColumn(x.fieldInfo.quotedCqlColumn, x.buildType(Optional.empty()));
        }

        final Create.Options options = table.withOptions();

        if (entityProperty.clusteringColumns.size() > 0 || entityProperty.staticTTL.isPresent()) {
            entityProperty.clusteringColumns
                    .stream()
                    .map(x -> Tuple2.of(x.fieldInfo.quotedCqlColumn, (ClusteringColumnInfo) x.fieldInfo.columnInfo))
                    .forEach(x -> options.clusteringOrder(x._1(), x._2().direction));

            if (entityProperty.staticTTL.isPresent()) {
                options.defaultTimeToLive(entityProperty.staticTTL.get().intValue());
            }
            builder.append(options.getQueryString().replaceFirst("\t+", "")).append(";");
        } else {
            builder.append(table.getQueryString().replaceFirst("\t+", "")).append(";");
        }

        schemas.add(builder.toString());

        if (context.createIndex) {
            entityProperty.allColumns
                    .stream()
                    .filter(x -> x.fieldInfo.indexInfo.type != IndexType.NONE)
                    .map(x -> Tuple2.of(x.fieldInfo.quotedCqlColumn, x.fieldInfo.indexInfo))
                    .forEach(tuple -> schemas.add(tuple._2().generate(keyspace, tableName, tuple._1())));
        }

        return schemas;
    }

    public static List<String> generateView(SchemaContext context, AbstractViewProperty<?> viewProperty) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating materialized view for entity of type %s",
                    viewProperty.entityClass.getCanonicalName()));
        }

        final Optional<String> keyspace = Optional.ofNullable(context.keyspace.orElse(viewProperty.staticKeyspace.orElse(null)));
        final StringBuilder viewScript = new StringBuilder();
        final String viewName = viewProperty.getTableOrViewName();
        final Optional<String> staticKeyspace = viewProperty.getKeyspace();
        final Optional<String> overridenKeyspace = OverridingOptional
                .from(staticKeyspace)
                .andThen(keyspace)
                .getOptional();

        final AbstractEntityProperty<?> baseClassProperty = viewProperty.getBaseClassProperty();
        final String baseTableName;
        if (overridenKeyspace.isPresent()) {
            viewScript.append("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
                    .append(overridenKeyspace.get()).append(".").append(viewName)
                    .append("\n")
                    .append("AS SELECT ");
            baseTableName = overridenKeyspace.get() + "." + baseClassProperty.getTableOrViewName();
        } else {
            viewScript.append("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
                    .append(viewName)
                    .append("\n")
                    .append("AS SELECT ");
            baseTableName = baseClassProperty.getTableOrViewName();
        }


        final StringJoiner columns = new StringJoiner(",");
        viewProperty.allColumns.forEach(x -> columns.add(x.fieldInfo.quotedCqlColumn));

        viewScript.append(columns.toString()).append("\n");
        viewScript.append("FROM ").append(baseTableName).append("\n");

        final List<AbstractProperty<?, ?, ?>> viewPksProperty = new ArrayList<>(viewProperty.partitionKeys);
        viewPksProperty.addAll(viewProperty.clusteringColumns);

        final StringJoiner whereClause = new StringJoiner(" AND ");
        viewPksProperty.forEach(x -> whereClause.add(x.fieldInfo.quotedCqlColumn + " IS NOT NULL"));

        viewScript.append("WHERE ").append(whereClause).append("\n");

        final StringJoiner partitionKeys = new StringJoiner(",", "(", ")");
        final StringJoiner clusteringColumns = new StringJoiner(",");

        viewProperty.partitionKeys.forEach(x -> partitionKeys.add(x.fieldInfo.quotedCqlColumn));
        viewProperty.clusteringColumns.forEach(x -> clusteringColumns.add(x.fieldInfo.quotedCqlColumn));


        viewScript.append("PRIMARY KEY")
                .append("(")
                    .append(partitionKeys).append(",").append(clusteringColumns)
                .append(")");

        if (viewProperty.clusteringColumns.size() > 0) {
            StringJoiner clusteringOrder = new StringJoiner(",");
            viewProperty.clusteringColumns
                    .stream()
                    .forEach(x -> clusteringOrder.add(x.fieldInfo.quotedCqlColumn +
                            " " +
                            ((ClusteringColumnInfo) x.fieldInfo.columnInfo).clusteringOrder.name()));


            viewScript.append("\n")
                    .append("WITH CLUSTERING ORDER BY")
                    .append("(")
                    .append(clusteringOrder)
                    .append(")");
        }


        return Arrays.asList(viewScript.append(";").toString());
    }

    public static void generateSchemaAtRuntime(final Session session, AbstractEntityProperty<?> entityProperty) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating schema for entity of type %s",
                    entityProperty.entityClass.getCanonicalName()));
        }


        final String keyspace = entityProperty.getKeyspace().orElseGet(session::getLoggedKeyspace);
        final SchemaContext schemaContext = new SchemaContext(keyspace, true, true);
        final List<String> schemas;
        if (entityProperty.isTable()) {
            schemas = generateTable_And_Indices(schemaContext, entityProperty);
        } else {
            schemas = generateView(schemaContext, (AbstractViewProperty) entityProperty);
        }


        for(String schema: schemas) {
            if (ACHILLES_DML_LOGGER.isDebugEnabled()) {
                ACHILLES_DML_LOGGER.debug(schema + "\n");
            }
            final ResultSet resultSet = session.execute(schema);
            resultSet.getExecutionInfo().isSchemaInAgreement();
        }
    }

    public static void generateUDTAtRuntime(final Session session, AbstractUDTClassProperty<?> udtClassProperty) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating schema for udt of type %s",
                    udtClassProperty.udtClass.getCanonicalName()));
        }

        udtClassProperty.componentsProperty
                .stream()
                .flatMap(x -> x.getUDTClassProperties().stream())
                .forEach(x -> generateUDTAtRuntime(session, x));

        final String udtKeyspace = udtClassProperty.staticKeyspace.orElseGet(session::getLoggedKeyspace);
        final SchemaContext schemaContext = new SchemaContext(udtKeyspace, true, true);
        final String udtSchema = udtClassProperty.generateSchema(schemaContext);

        if (ACHILLES_DML_LOGGER.isDebugEnabled()) {
            ACHILLES_DML_LOGGER.debug(udtSchema + "\n");
        }

        final ResultSet resultSet = session.execute(udtSchema);
        resultSet.getExecutionInfo().isSchemaInAgreement();
    }
}
