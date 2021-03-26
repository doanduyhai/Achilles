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

package info.archinnov.achilles.internals.parser.context;

import javax.lang.model.element.VariableElement;

import info.archinnov.achilles.annotations.SASI.Analyzer;
import info.archinnov.achilles.annotations.SASI.IndexMode;
import info.archinnov.achilles.annotations.SASI.Normalization;

public class SASIInfoContext implements IndexInfoHelper{
    public final String indexName;
    public final IndexMode indexMode;
    public final boolean analyzed;
    public final Analyzer analyzerClass;
    public final int maxCompactionFlushMemoryInMb;
    public final Normalization normalization;
    public final String locale;
    public final boolean enableStemming;
    public final boolean skipStopWords;

    public SASIInfoContext(String indexName, IndexMode indexMode, boolean analyzed, Analyzer analyzerClass, int maxCompactionFlushMemoryInMb, Normalization normalization, String locale, boolean enableStemming, boolean skipStopWords) {
        this.indexName = indexName;
        this.indexMode = indexMode;
        this.analyzed = analyzed;
        this.analyzerClass = analyzerClass;
        this.maxCompactionFlushMemoryInMb = maxCompactionFlushMemoryInMb;
        this.normalization = normalization;
        this.locale = locale;
        this.enableStemming = enableStemming;
        this.skipStopWords = skipStopWords;
    }

    public SASIInfoContext build(VariableElement elm, EntityParsingContext context) {
        String newIndexName = computeIndexName(indexName, elm, context);
        return new SASIInfoContext(newIndexName, indexMode, analyzed, analyzerClass, maxCompactionFlushMemoryInMb, normalization, locale, enableStemming, skipStopWords);
    }
}
