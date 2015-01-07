/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.test.parser.entity;

import java.util.UUID;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Column;

public class EmbeddedKeyChild3 extends EmbeddedKeyChild2 {

    @ClusteringColumn
    @Column(name = "clustering_key")
    private UUID clustering;

    public UUID getClustering() {
        return clustering;
    }

    public void setClustering(UUID clustering) {
        this.clustering = clustering;
    }
}
