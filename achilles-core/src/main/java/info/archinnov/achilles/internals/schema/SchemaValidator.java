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

package info.archinnov.achilles.internals.schema;

import static info.archinnov.achilles.internals.metamodel.index.IndexType.*;
import static info.archinnov.achilles.validation.Validator.validateBeanMappingFalse;
import static info.archinnov.achilles.validation.Validator.validateBeanMappingTrue;
import static java.lang.String.format;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.TableMetadata;

import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;

public class SchemaValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidator.class);

    public static void validateDefaultTTL(TableMetadata metadata, Optional<Integer> staticTTL, Class<?> entityClass) {
        if (staticTTL.isPresent()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Validating table %s default TTL value",
                        metadata.getName()));
            }
            final int defaultTimeToLive = metadata.getOptions().getDefaultTimeToLive();
            validateBeanMappingTrue(staticTTL.get().equals(defaultTimeToLive),
                    "Default TTL '%s' declared on entity '%s' does not match detected default TTL '%s' in live schema",
                    staticTTL.get(), entityClass.getCanonicalName(), defaultTimeToLive);
        }
    }

    public static <T> void validateColumns(TableMetadata metadata, List<AbstractProperty<T, ?, ?>> properties,
                                           Class<T> entityClass) {

        for (AbstractProperty<T, ?, ?> x : properties) {
            final String cqlColumn = x.fieldInfo.cqlColumn;
            final ColumnMetadata columnMeta = metadata.getColumn(cqlColumn);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Validating column %s for table %s",
                        cqlColumn, metadata.getName()));
            }

            validateBeanMappingTrue(columnMeta != null,
                    "Cannot find column '%s' in live schema for entity '%s'", cqlColumn, entityClass);

            final DataType runtimeType = columnMeta.getType();
            final DataType staticType = x.buildType();
            validateBeanMappingTrue(runtimeType.equals(staticType),
                    "Data type '%s' for column '%s' of entity '%s' does not match type in live schema '%s'",
                    staticType, cqlColumn, entityClass, runtimeType);

            if (x.fieldInfo.hasIndex()) {
                validateIndex(entityClass, x, cqlColumn, metadata.getIndex(x.fieldInfo.indexInfo.name));
            }

            if (x.fieldInfo.columnType == ColumnType.STATIC || x.fieldInfo.columnType == ColumnType.STATIC_COUNTER) {
                validateBeanMappingTrue(columnMeta.isStatic(), "Column '%s' of entity '%s' should be static", cqlColumn, entityClass);
            }
        }
    }

    private static void validateIndex(Class<?> entityClass, AbstractProperty<?, ?, ?> x, String cqlColumn, IndexMetadata indexMetadata) {
        final IndexInfo indexInfo = x.fieldInfo.indexInfo;


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Validating index on column % of table %s",
                    cqlColumn, indexMetadata.getTable().getName()));
        }

        validateBeanMappingTrue(indexInfo.name.equals(indexMetadata.getName()),
                "Index name '%s' for column '%s' of entity '%s' does not match name '%s' in live schema",
                indexInfo.name, cqlColumn, entityClass, indexMetadata.getName());

        final String indexTarget = indexMetadata.getTarget().toLowerCase();
        final boolean isIndexOnFullCollection = indexTarget.contains(format("full(%s)", indexInfo.name));
        final boolean isIndexOnMapEntries = indexTarget.contains(format("entries(%s)", indexInfo.name));
        final boolean isIndexOnMapKeys = indexTarget.contains(format("keys(%s)", indexInfo.name));
        final boolean isCustomIndex = indexMetadata.isCustomIndex();

        switch (indexInfo.type) {
            case NORMAL:
                validateBeanMappingFalse(isIndexOnMapEntries,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        NORMAL, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnFullCollection,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        NORMAL, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnMapKeys,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        NORMAL, cqlColumn, entityClass);
                validateBeanMappingFalse(isCustomIndex,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        NORMAL, cqlColumn, entityClass);
                break;
            case COLLECTION:
                validateBeanMappingFalse(isIndexOnMapEntries,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        COLLECTION, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnFullCollection,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        COLLECTION, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnMapKeys,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        COLLECTION, cqlColumn, entityClass);
                validateBeanMappingFalse(isCustomIndex,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        COLLECTION, cqlColumn, entityClass);
                break;
            case FULL:
                validateBeanMappingTrue(isIndexOnFullCollection,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        FULL, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnMapEntries,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        FULL, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnMapKeys,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        FULL, cqlColumn, entityClass);
                validateBeanMappingFalse(isCustomIndex,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        FULL, cqlColumn, entityClass);
                break;
            case MAP_ENTRY:
                validateBeanMappingTrue(isIndexOnMapEntries,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_ENTRY, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnFullCollection,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_ENTRY, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnMapKeys,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_ENTRY, cqlColumn, entityClass);
                validateBeanMappingFalse(isCustomIndex,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_ENTRY, cqlColumn, entityClass);
                break;
            case MAP_KEY:
                validateBeanMappingFalse(isIndexOnMapEntries,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_KEY, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnFullCollection,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_KEY, cqlColumn, entityClass);
                validateBeanMappingTrue(isIndexOnMapKeys,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_KEY, cqlColumn, entityClass);
                validateBeanMappingFalse(isCustomIndex,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        MAP_KEY, cqlColumn, entityClass);
                break;
            case CUSTOM:
                validateBeanMappingFalse(isIndexOnMapEntries,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        CUSTOM, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnFullCollection,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        CUSTOM, cqlColumn, entityClass);
                validateBeanMappingFalse(isIndexOnMapKeys,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        CUSTOM, cqlColumn, entityClass);
                validateBeanMappingTrue(isCustomIndex,
                        "Index type '%s' for column '%s' of entity '%s' does not match type in live schema",
                        CUSTOM, cqlColumn, entityClass);
        }
    }
}
