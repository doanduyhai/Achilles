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

package info.archinnov.achilles.internals.sample_classes.parser.field;

import static info.archinnov.achilles.annotations.SASI.Analyzer.*;
import static info.archinnov.achilles.annotations.SASI.IndexMode.PREFIX;
import static info.archinnov.achilles.annotations.SASI.IndexMode.SPARSE;
import static info.archinnov.achilles.annotations.SASI.Normalization.LOWERCASE;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.SASI;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;


@APUnitTest
public class TestEntityForSASI {


    @SASI(indexMode = PREFIX, analyzerClass = NON_TOKENIZING_ANALYZER, analyzed = true)
    @Column
    private String prefixNonTokenizer;

    @SASI(analyzed = true)
    @Column
    private Long analyzedNotString;

    @SASI(analyzed = true, indexMode = SPARSE)
    @Column
    private String analyzedSparse;

    @SASI(analyzed = true, analyzerClass = NO_OP_ANALYZER)
    @Column
    private String analyzedNoOpAnalyzer;

    @SASI(analyzed = true, analyzerClass = STANDARD_ANALYZER)
    @Column
    private Long standardAnalyzerNotString;

    @SASI(analyzed = false, analyzerClass = STANDARD_ANALYZER, indexMode = SPARSE)
    @Column
    private String standardAnalyzerSparse;

    @SASI(analyzed = false, analyzerClass = STANDARD_ANALYZER)
    @Column
    private String standardAnalyzerNotAnalyzed;

    @SASI(analyzed = true, analyzerClass = NON_TOKENIZING_ANALYZER, normalization = LOWERCASE)
    @Column
    private Long normalizationNotString;

    @SASI(analyzed = false, analyzerClass = NO_OP_ANALYZER, normalization = LOWERCASE, indexMode = SPARSE)
    @Column
    private String normalizationSparse;

    @SASI(analyzed = false, analyzerClass = NO_OP_ANALYZER, normalization = LOWERCASE)
    @Column
    private String normalizationNotAnalyzed;

    @SASI(analyzed = true, analyzerClass = NO_OP_ANALYZER, normalization = LOWERCASE)
    @Column
    private String normalizationNoAnalyzer;

    @SASI(enableStemming = true)
    @Column
    private Long stemmingNotString;

    @SASI(enableStemming = true, indexMode = SPARSE)
    @Column
    private String stemmingSparse;

    @SASI(enableStemming = true, analyzed = false)
    @Column
    private String stemmingNotAnalyzed;

    @SASI(enableStemming = true, analyzed = true, analyzerClass = NON_TOKENIZING_ANALYZER)
    @Column
    private String stemmingNonTokenizingAnalyzer;

    @SASI(indexMode = SPARSE)
    @Column
    private String sparsedButString;

    @SASI
    @Column
    private List<String> indexedList;

    @SASI
    @Column
    private Set<String> indexedSet;

    @SASI
    @Column
    private Map<Integer,String> indexedMap;

    public String getPrefixNonTokenizer() {
        return prefixNonTokenizer;
    }

    public void setPrefixNonTokenizer(String prefixNonTokenizer) {
        this.prefixNonTokenizer = prefixNonTokenizer;
    }

    public Long getAnalyzedNotString() {
        return analyzedNotString;
    }

    public void setAnalyzedNotString(Long analyzedNotString) {
        this.analyzedNotString = analyzedNotString;
    }

    public String getAnalyzedSparse() {
        return analyzedSparse;
    }

    public void setAnalyzedSparse(String analyzedSparse) {
        this.analyzedSparse = analyzedSparse;
    }

    public String getAnalyzedNoOpAnalyzer() {
        return analyzedNoOpAnalyzer;
    }

    public void setAnalyzedNoOpAnalyzer(String analyzedNoOpAnalyzer) {
        this.analyzedNoOpAnalyzer = analyzedNoOpAnalyzer;
    }

    public Long getStandardAnalyzerNotString() {
        return standardAnalyzerNotString;
    }

    public void setStandardAnalyzerNotString(Long standardAnalyzerNotString) {
        this.standardAnalyzerNotString = standardAnalyzerNotString;
    }

    public String getStandardAnalyzerSparse() {
        return standardAnalyzerSparse;
    }

    public void setStandardAnalyzerSparse(String standardAnalyzerSparse) {
        this.standardAnalyzerSparse = standardAnalyzerSparse;
    }

    public String getStandardAnalyzerNotAnalyzed() {
        return standardAnalyzerNotAnalyzed;
    }

    public void setStandardAnalyzerNotAnalyzed(String standardAnalyzerNotAnalyzed) {
        this.standardAnalyzerNotAnalyzed = standardAnalyzerNotAnalyzed;
    }

    public Long getNormalizationNotString() {
        return normalizationNotString;
    }

    public void setNormalizationNotString(Long normalizationNotString) {
        this.normalizationNotString = normalizationNotString;
    }

    public String getNormalizationSparse() {
        return normalizationSparse;
    }

    public void setNormalizationSparse(String normalizationSparse) {
        this.normalizationSparse = normalizationSparse;
    }

    public String getNormalizationNotAnalyzed() {
        return normalizationNotAnalyzed;
    }

    public void setNormalizationNotAnalyzed(String normalizationNotAnalyzed) {
        this.normalizationNotAnalyzed = normalizationNotAnalyzed;
    }

    public String getNormalizationNoAnalyzer() {
        return normalizationNoAnalyzer;
    }

    public void setNormalizationNoAnalyzer(String normalizationNoAnalyzer) {
        this.normalizationNoAnalyzer = normalizationNoAnalyzer;
    }

    public Long getStemmingNotString() {
        return stemmingNotString;
    }

    public void setStemmingNotString(Long stemmingNotString) {
        this.stemmingNotString = stemmingNotString;
    }

    public String getStemmingSparse() {
        return stemmingSparse;
    }

    public void setStemmingSparse(String stemmingSparse) {
        this.stemmingSparse = stemmingSparse;
    }

    public String getStemmingNotAnalyzed() {
        return stemmingNotAnalyzed;
    }

    public void setStemmingNotAnalyzed(String stemmingNotAnalyzed) {
        this.stemmingNotAnalyzed = stemmingNotAnalyzed;
    }

    public String getStemmingNonTokenizingAnalyzer() {
        return stemmingNonTokenizingAnalyzer;
    }

    public void setStemmingNonTokenizingAnalyzer(String stemmingNonTokenizingAnalyzer) {
        this.stemmingNonTokenizingAnalyzer = stemmingNonTokenizingAnalyzer;
    }

    public String getSparsedButString() {
        return sparsedButString;
    }

    public void setSparsedButString(String sparsedButString) {
        this.sparsedButString = sparsedButString;
    }

    public List<String> getIndexedList() {
        return indexedList;
    }

    public void setIndexedList(List<String> indexedList) {
        this.indexedList = indexedList;
    }

    public Set<String> getIndexedSet() {
        return indexedSet;
    }

    public void setIndexedSet(Set<String> indexedSet) {
        this.indexedSet = indexedSet;
    }

    public Map<Integer, String> getIndexedMap() {
        return indexedMap;
    }

    public void setIndexedMap(Map<Integer, String> indexedMap) {
        this.indexedMap = indexedMap;
    }
}
