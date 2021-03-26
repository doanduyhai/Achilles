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

package info.archinnov.achilles.internals.metamodel.index;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Optional;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.annotations.SASI.Analyzer;
import info.archinnov.achilles.annotations.SASI.IndexMode;
import info.archinnov.achilles.annotations.SASI.Normalization;
import info.archinnov.achilles.internals.parser.context.DSESearchInfoContext;
import info.archinnov.achilles.internals.parser.context.SASIInfoContext;

public class IndexInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexInfo.class);

    public final IndexImpl impl;
    public final IndexType type;
    public final String name;
    public final Optional<String> indexClassName;
    public final Optional<String> indexOptions;
    public final Optional<SASIInfoContext> sasiInfoContext;
    public final Optional<DSESearchInfoContext> dseSearchInfoContext;

    private IndexInfo(IndexImpl indexImpl, IndexType type, String name, String indexClassName, String indexOptions,
                      Optional<SASIInfoContext> sasiInfoContext,
                      Optional<DSESearchInfoContext> dseSearchInfoContext) {
        this.impl = indexImpl;
        this.type = type;
        this.name = name;
        this.sasiInfoContext = sasiInfoContext;
        this.dseSearchInfoContext = dseSearchInfoContext;
        this.indexClassName = isNotBlank(indexClassName) ? Optional.of(indexClassName): Optional.empty();
        this.indexOptions = isNotBlank(indexOptions) ? Optional.of(indexOptions): Optional.empty();
    }

    public static IndexInfo forNative(IndexType indexType, String indexName, String indexClassName, String indexOptions) {
        return new IndexInfo(IndexImpl.NATIVE, indexType, indexName, indexClassName, indexOptions,
                Optional.empty(), Optional.empty());
    }

    public static IndexInfo forSASI(String indexName,
                                    IndexMode indexMode,
                                    boolean analyzed,
                                    Analyzer analyzer,
                                    int maxCompactionFlushMemoryInMb,
                                    Normalization normalization,
                                    String locale,
                                    boolean enableStemming,
                                    boolean skipStopWords) {

        String options = buildOptionsForSASI(indexMode, analyzed, analyzer, maxCompactionFlushMemoryInMb, normalization, locale, enableStemming, skipStopWords);
        final SASIInfoContext sasiInfoContext = new SASIInfoContext(indexName, indexMode, analyzed, analyzer, maxCompactionFlushMemoryInMb, normalization, locale, enableStemming, skipStopWords);
        return new IndexInfo(IndexImpl.SASI, IndexType.NORMAL, indexName, "org.apache.cassandra.index.sasi.SASIIndex", options,
                Optional.of(sasiInfoContext),
                Optional.empty());
    }

    public static IndexInfo forDSESearch(boolean fullTextSearchEnable) {
        return new IndexInfo(IndexImpl.DSE_SEARCH, IndexType.NORMAL,
                "",
                DSESearchInfoContext.DSE_SEARCH_INDEX_CLASSNAME,
                "",
                Optional.empty(),
                Optional.of(new DSESearchInfoContext(fullTextSearchEnable)));
    }

    private static String buildOptionsForSASI(IndexMode indexMode, boolean analyzed, Analyzer analyzer, int maxCompactionFlushMemoryInMb, Normalization normalization, String locale, boolean enableStemming, boolean skipStopWords) {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(format("'max_compaction_flush_memory_in_mb' : '%s'", maxCompactionFlushMemoryInMb));
        joiner.add(format("'mode' : '%s'", indexMode.name()));
        if (analyzed) {
            joiner.add(format("'analyzed' : '%s'", analyzed));
            joiner.add(format("'analyzer_class' : '%s'", analyzer.analyzerClass()));
            if (analyzer == Analyzer.STANDARD_ANALYZER) {
                joiner.add(format("'tokenization_locale': '%s'", locale.toLowerCase()));
                joiner.add(format("'tokenization_enable_stemming': '%s'", enableStemming));
                joiner.add(format("'tokenization_skip_stop_words': '%s'", skipStopWords));
                if(normalization != Normalization.NONE) joiner.add(format("'%s': 'true'", normalization.forStandardAnalyzer()));
            } else if (analyzer == Analyzer.NON_TOKENIZING_ANALYZER) {
                if (normalization == Normalization.NONE) {
                    joiner.add(format("'case_sensitive': 'true'"));
                } else {
                    joiner.add(format("'%s': 'true'", normalization.forNonTokenizingAnalyzer()));
                }
            }
        }
        return joiner.toString();
    }


    public static IndexInfo noIndex() {
        return new IndexInfo(IndexImpl.NATIVE, IndexType.NONE, null, null, null, Optional.empty(), Optional.empty());
    }

    public String generate(Optional<String> keyspace, String table, String columnName) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating index creation script for column %s in table %s of keyspace %s",
                    columnName, table, keyspace));
        }

        switch (impl) {
            case NATIVE:
                return generateNativeIndex(keyspace, table, columnName);
            case SASI:
                return generateSASI(keyspace, table, columnName);
            default:
                return "";
        }

    }

    private String generateSASI(Optional<String> keyspace, String table, String columnName) {
        String template = "\nCREATE CUSTOM INDEX IF NOT EXISTS %s ON %s ( %s )  USING '%s' WITH OPTIONS = {%s};";
        String tableWithKeyspace = table;
        if (keyspace.isPresent()) {
            tableWithKeyspace = keyspace.get() + "." + table;
        }
        return format(template, name, tableWithKeyspace, columnName, indexClassName.get(), indexOptions.get());
    }

    private String generateNativeIndex(Optional<String> keyspace, String table, String columnName) {
        String template = "\nCREATE %s INDEX IF NOT EXISTS %s ON %s ( %s )%s%s;";
        String indexType;
        String tableWithKeyspace = table;
        String indexClassString = "";
        String custom = "";
        StringBuilder indexOptionsString = new StringBuilder("");

        switch (type) {
            case FULL:
                indexType = "FULL(" + columnName + ")";
                break;
            case MAP_ENTRY:
                indexType = "ENTRIES(" + columnName + ")";
                break;
            case MAP_KEY:
                indexType = "KEYS(" + columnName + ")";
                break;
            default:
                indexType = columnName;
        }

        if (keyspace.isPresent()) {
            tableWithKeyspace = keyspace.get() + "." + table;
        }

        if (indexClassName.isPresent()) {
            custom = "CUSTOM";
            indexClassString = " USING '" + indexClassName.get() + "'";
            indexOptions.ifPresent(x -> {
                if (isNotBlank(x)) {
                    indexOptionsString.append(" WITH OPTIONS = '").append(x).append("'");
                }
            });
        }
        return format(template, custom, name, tableWithKeyspace, indexType, indexClassString,
                indexOptionsString.toString());
    }
}
