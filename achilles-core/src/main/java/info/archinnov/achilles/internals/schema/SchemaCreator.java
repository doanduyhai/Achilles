/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.AbstractUDTClassProperty;
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
        final String tableName = entityProperty.getTableName();
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
            table.addPartitionKey(x.fieldInfo.cqlColumn, x.buildType());
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.clusteringColumns) {
            table.addClusteringColumn(x.fieldInfo.cqlColumn, x.buildType());
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.staticColumns) {
            table.addStaticColumn(x.fieldInfo.cqlColumn, x.buildType());
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.normalColumns) {
            table.addColumn(x.fieldInfo.cqlColumn, x.buildType());
        }

        for (AbstractProperty<?, ?, ?> x : entityProperty.counterColumns) {
            table.addColumn(x.fieldInfo.cqlColumn, x.buildType());
        }

        final Create.Options options = table.withOptions();

        if (entityProperty.clusteringColumns.size() > 0 || entityProperty.staticTTL.isPresent()) {
            entityProperty.clusteringColumns
                    .stream()
                    .map(x -> Tuple2.of(x.fieldInfo.cqlColumn, (ClusteringColumnInfo) x.fieldInfo.columnInfo))
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
                    .map(x -> Tuple2.of(x.fieldInfo.cqlColumn, x.fieldInfo.indexInfo))
                    .forEach(tuple -> schemas.add(tuple._2().generate(keyspace, tableName, tuple._1())));
        }

        return schemas;
    }

    public static void generateSchemaAtRuntime(final Session session, AbstractEntityProperty<?> entityProperty) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating schema for entity of type %s",
                    entityProperty.entityClass.getCanonicalName()));
        }


        final String keyspace = entityProperty.getKeyspace().orElse(session.getLoggedKeyspace());
        final SchemaContext schemaContext = new SchemaContext(keyspace, true, true);
        final List<String> schemas = generateTable_And_Indices(schemaContext, entityProperty);

        for(String schema: schemas) {
            if (ACHILLES_DML_LOGGER.isDebugEnabled()) {
                ACHILLES_DML_LOGGER.debug(schema);
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
        final String udtKeyspace = udtClassProperty.staticKeyspace.orElse(session.getLoggedKeyspace());
        final String udtSchema = udtClassProperty.generateSchema(new SchemaContext(udtKeyspace, true, true));
        final ResultSet resultSet = session.execute(udtSchema);
        resultSet.getExecutionInfo().isSchemaInAgreement();
    }

}
