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

import static info.archinnov.achilles.annotations.SASI.Analyzer.NON_TOKENIZING_ANALYZER;
import static info.archinnov.achilles.annotations.SASI.Analyzer.STANDARD_ANALYZER;
import static info.archinnov.achilles.internals.metamodel.index.IndexType.*;
import static info.archinnov.achilles.validation.Validator.validateBeanMappingFalse;
import static info.archinnov.achilles.validation.Validator.validateBeanMappingTrue;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;

import info.archinnov.achilles.annotations.SASI;
import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.index.IndexImpl;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.parser.context.DSESearchInfoContext;
import info.archinnov.achilles.internals.parser.context.SASIInfoContext;

public class SchemaValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidator.class);

    public static void validateDefaultTTL(AbstractTableMetadata metadata, Optional<Integer> staticTTL, Class<?> entityClass) {
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

    public static <T> void validateColumnType(ColumnType columnType, AbstractTableMetadata metadata,
                                              List<AbstractProperty<T, ?, ?>> properties, Class<T> entityClass) {
        final List<String> mappedColumnNames = properties.stream().map(x -> x.fieldInfo.cqlColumn).collect(toList());
        final String className = entityClass.getCanonicalName();
        switch (columnType) {
            case PARTITION:
                final List<String> partitionKeyColumnNames = metadata.getPartitionKey().stream()
                        .map(ColumnMetadata::getName)
                        .collect(toList());
                validateBeanMappingTrue(isEqualCollection(mappedColumnNames, partitionKeyColumnNames),
                    "The mapped partition key(s) %s for entity %s do not correspond to live schema partition key(s) %s",
                    mappedColumnNames.stream().collect(Collectors.joining(", ", "[", "]")),
                    className,
                    partitionKeyColumnNames.stream().collect(Collectors.joining(", ", "[", "]")));
                return;
            case CLUSTERING:
                final List<String> clusteringColColumnNames = metadata.getClusteringColumns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(toList());
                validateBeanMappingTrue(isEqualCollection(mappedColumnNames, clusteringColColumnNames),
                    "The mapped clustering column(s) %s for entity %s do not correspond to live schema clustering column(s) %s",
                    mappedColumnNames.stream().collect(Collectors.joining(", ", "[", "]")),
                    className,
                    clusteringColColumnNames.stream().collect(Collectors.joining(", ", "[", "]")));
                return;
            case STATIC:
                final List<String> staticColColumnNames = metadata.getColumns().stream()
                        .filter(ColumnMetadata::isStatic)
                        .map(ColumnMetadata::getName)
                        .collect(toList());
                validateBeanMappingTrue(isEqualCollection(mappedColumnNames, staticColColumnNames),
                    "The mapped static column(s) %s for entity %s do not correspond to live schema static column(s) %s",
                    mappedColumnNames.stream().collect(Collectors.joining(", ", "[", "]")),
                    className,
                    staticColColumnNames.stream().collect(Collectors.joining(", ", "[", "]")));
                return;
            default:
                return;
        }
    }

    public static <T> void validateColumns(AbstractTableMetadata metadata, List<AbstractProperty<T, ?, ?>> properties,
                                           Class<T> entityClass) {

        for (AbstractProperty<T, ?, ?> x : properties) {
            final String cqlColumn = x.fieldInfo.quotedCqlColumn;
            final ColumnMetadata columnMeta = metadata.getColumn(cqlColumn);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Validating column %s for table %s",
                        cqlColumn, metadata.getName()));
            }

            validateBeanMappingTrue(columnMeta != null,
                    "Cannot find column '%s' in live schema for entity '%s'", cqlColumn, entityClass);

            final DataType runtimeType = columnMeta.getType();
            final DataType staticType = x.buildType(Optional.empty());
            validateBeanMappingTrue(runtimeType.equals(staticType),
                    "Data type '%s' for column '%s' of entity '%s' does not match type in live schema '%s'",
                    staticType, cqlColumn, entityClass, runtimeType);

            if (x.fieldInfo.hasIndex()) {
                final TableMetadata tableMetadata = (TableMetadata) metadata;
                final String indexName = x.fieldInfo.indexInfo.name;
                final IndexMetadata indexMetadata = tableMetadata.getIndex(indexName);

                if (x.fieldInfo.indexInfo.impl == IndexImpl.NATIVE) {
                    validateBeanMappingTrue(indexMetadata != null, "Cannot find index name '%s' for column '%s' of entity '%s' in live schema",
                            indexName, cqlColumn, entityClass);
                    validateNativeIndex(entityClass, x, cqlColumn, indexMetadata);
                } else if (x.fieldInfo.indexInfo.impl == IndexImpl.SASI) {
                    validateBeanMappingTrue(indexMetadata != null, "Cannot find index name '%s' for column '%s' of entity '%s' in live schema",
                            indexName, cqlColumn, entityClass);
                    validateSASIIndex(entityClass, x, cqlColumn, indexMetadata);
                } else if (x.fieldInfo.indexInfo.impl == IndexImpl.DSE_SEARCH) {
                    validateDSESearchIndex(entityClass, tableMetadata);
                }
            }

            if (x.fieldInfo.columnType == ColumnType.STATIC || x.fieldInfo.columnType == ColumnType.STATIC_COUNTER) {
                validateBeanMappingTrue(columnMeta.isStatic(), "Column '%s' of entity '%s' should be static", cqlColumn, entityClass);
            }
        }
    }

    private static void validateDSESearchIndex(Class<?> entityClass, TableMetadata tableMetadata) {
        final String tableName = tableMetadata.getName().toLowerCase();
        final String keyspaceName = tableMetadata.getKeyspace().getName().toLowerCase();
        final String indexName = keyspaceName + "_" + tableName + "_solr_query_index";
        final Optional<IndexMetadata> indexMeta = Optional.ofNullable(tableMetadata.getIndex(indexName));

        validateBeanMappingTrue(indexMeta.isPresent(),
                "Index name %s for entity '%s' cannot be found", indexName, entityClass);

        final IndexMetadata indexMetadata = indexMeta.get();
        final String indexClassName = indexMetadata.getIndexClassName();
        validateBeanMappingTrue(indexClassName.equals(DSESearchInfoContext.DSE_SEARCH_INDEX_CLASSNAME),
                "Index class name %s for entity '%s' should be %s",
                indexClassName, entityClass, DSESearchInfoContext.DSE_SEARCH_INDEX_CLASSNAME);
    }

    private static void validateSASIIndex(Class<?> entityClass, AbstractProperty<?, ?, ?> x, String cqlColumn, IndexMetadata indexMetadata) {
        final SASIInfoContext sasiInfo = x.fieldInfo.indexInfo.sasiInfoContext.get();
        final String indexName = sasiInfo.indexName;

        validateBeanMappingTrue(sasiInfo.indexMode.name().equals(indexMetadata.getOption("mode")),
                "Index name %s for column '%s' of entity '%s' should have option 'mode' = %s",
                indexName, cqlColumn, entityClass, indexMetadata.getOption("mode"));

        validateBeanMappingTrue((sasiInfo.maxCompactionFlushMemoryInMb + "").equals(indexMetadata.getOption("max_compaction_flush_memory_in_mb")),
                "Index name %s for column '%s' of entity '%s' should have option 'max_compaction_flush_memory_in_mb' = %s",
                indexName, cqlColumn, entityClass, indexMetadata.getOption("max_compaction_flush_memory_in_mb"));

        if (sasiInfo.analyzed) {
            validateBeanMappingTrue(indexMetadata.getOption("analyzed").equals("true"),
                    "Index name %s for column '%s' of entity '%s' should have option 'analyzed' = true",
                    indexName, cqlColumn, entityClass);

            validateBeanMappingTrue(sasiInfo.analyzerClass.analyzerClass().equals(indexMetadata.getOption("analyzer_class")),
                "Index name %s for column '%s' of entity '%s' should have option 'analyzerClass' = %s",
                indexName, cqlColumn, entityClass, indexMetadata.getOption("analyzerClass"));

            if (sasiInfo.analyzerClass == STANDARD_ANALYZER) {
                validateBeanMappingTrue(sasiInfo.locale.trim().toLowerCase().equals(indexMetadata.getOption("tokenization_locale")),
                        "Index name %s for column '%s' of entity '%s' should have option 'tokenization_locale' = %s",
                        indexName, cqlColumn, entityClass, indexMetadata.getOption("tokenization_locale"));

                validateBeanMappingTrue((sasiInfo.enableStemming + "").equals(indexMetadata.getOption("tokenization_enable_stemming")),
                        "Index name %s for column '%s' of entity '%s' should have option 'tokenization_enable_stemming' = %s",
                        indexName, cqlColumn, entityClass, indexMetadata.getOption("tokenization_enable_stemming"));

                validateBeanMappingTrue((sasiInfo.skipStopWords + "").equals(indexMetadata.getOption("tokenization_skip_stop_words")),
                        "Index name %s for column '%s' of entity '%s' should have option 'tokenization_skip_stop_words' = %s",
                        indexName, cqlColumn, entityClass, indexMetadata.getOption("tokenization_skip_stop_words"));

                final String normalization = sasiInfo.normalization.forStandardAnalyzer();
                final String liveNormalization = indexMetadata.getOption(normalization);
                validateBeanMappingTrue(isNotBlank(liveNormalization) && liveNormalization.equals("true"),
                        "Index name %s for column '%s' of entity '%s' should have option '%s' = true",
                        indexName, cqlColumn, entityClass, normalization);
            } else if (sasiInfo.analyzerClass == NON_TOKENIZING_ANALYZER) {
                if (sasiInfo.normalization == SASI.Normalization.NONE) {
                    final String liveCasseSensitive = indexMetadata.getOption("case_sensitive");
                    validateBeanMappingTrue(isNotBlank(liveCasseSensitive) && liveCasseSensitive.equals("true"),
                            "Index name %s for column '%s' of entity '%s' should have option 'case_sensitive' = true",
                            indexName, cqlColumn, entityClass);
                } else {
                    final String normalization = sasiInfo.normalization.forNonTokenizingAnalyzer();
                    final String liveNormalization = indexMetadata.getOption(normalization);
                    validateBeanMappingTrue(isNotBlank(liveNormalization) && liveNormalization.equals("true"),
                            "Index name %s for column '%s' of entity '%s' should have option '%s' = true",
                            indexName, cqlColumn, entityClass, normalization);
                }
            }

        }
    }

    private static void validateNativeIndex(Class<?> entityClass, AbstractProperty<?, ?, ?> x, String cqlColumn, IndexMetadata indexMetadata) {
        final IndexInfo indexInfo = x.fieldInfo.indexInfo;


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Validating index on column %s of table %s",
                    cqlColumn, indexMetadata.getTable().getName()));
        }

        final String indexName = indexInfo.name;
        validateBeanMappingTrue(indexName.equals(indexMetadata.getName()),
                "Index name '%s' for column '%s' of entity '%s' does not match name '%s' in live schema",
                indexName, cqlColumn, entityClass, indexMetadata.getName());

        final String indexTarget = indexMetadata.getTarget().toLowerCase();
        final boolean isIndexOnFullCollection = indexTarget.contains(format("full(%s)", cqlColumn));
        final boolean isIndexOnMapEntries = indexTarget.contains(format("entries(%s)", cqlColumn));
        final boolean isIndexOnMapKeys = indexTarget.contains(format("keys(%s)", cqlColumn));
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
