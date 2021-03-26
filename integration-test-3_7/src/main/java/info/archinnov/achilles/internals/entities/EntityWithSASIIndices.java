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

package info.archinnov.achilles.internals.entities;

import static info.archinnov.achilles.annotations.SASI.Analyzer.NON_TOKENIZING_ANALYZER;
import static info.archinnov.achilles.annotations.SASI.Analyzer.STANDARD_ANALYZER;
import static info.archinnov.achilles.annotations.SASI.IndexMode.*;
import static info.archinnov.achilles.annotations.SASI.Normalization.LOWERCASE;

import info.archinnov.achilles.annotations.*;

@Table(keyspace = "it_3_7", table = "entity_with_sasi_indices")
public class EntityWithSASIIndices {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    private int clust;

    @SASI(indexMode = PREFIX, analyzed = true, analyzerClass = NON_TOKENIZING_ANALYZER, normalization = LOWERCASE)
    @Column
    private String prefixNonTokenizer;

    @SASI(indexMode = CONTAINS, analyzed = true, analyzerClass = STANDARD_ANALYZER, normalization = LOWERCASE, enableStemming = true)
    @Column
    private String containsStandardAnalyzer;

    @SASI
    @Column
    private int numeric;

    @SASI(indexMode = SPARSE)
    @Column
    private int sparse;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getClust() {
        return clust;
    }

    public void setClust(int clust) {
        this.clust = clust;
    }

    public String getPrefixNonTokenizer() {
        return prefixNonTokenizer;
    }

    public void setPrefixNonTokenizer(String prefixNonTokenizer) {
        this.prefixNonTokenizer = prefixNonTokenizer;
    }

    public String getContainsStandardAnalyzer() {
        return containsStandardAnalyzer;
    }

    public void setContainsStandardAnalyzer(String containsStandardAnalyzer) {
        this.containsStandardAnalyzer = containsStandardAnalyzer;
    }

    public int getNumeric() {
        return numeric;
    }

    public void setNumeric(int numeric) {
        this.numeric = numeric;
    }

    public int getSparse() {
        return sparse;
    }

    public void setSparse(int sparse) {
        this.sparse = sparse;
    }
}
