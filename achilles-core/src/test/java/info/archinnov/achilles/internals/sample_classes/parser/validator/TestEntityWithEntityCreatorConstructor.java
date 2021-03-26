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

package info.archinnov.achilles.internals.sample_classes.parser.validator;

import java.util.UUID;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
public class TestEntityWithEntityCreatorConstructor {

    @PartitionKey
    private Long partition;

    @ClusteringColumn
    private UUID clustering;

    @Column
    private String value;

    public TestEntityWithEntityCreatorConstructor() {
    }

    @EntityCreator
    public TestEntityWithEntityCreatorConstructor(Long partition, UUID clustering, String value) {
        this.partition = partition;
        this.clustering = clustering;
        this.value = value;
    }

    public Long getPartition() {
        return partition;
    }

    public UUID getClustering() {
        return clustering;
    }

    public String getValue() {
        return value;
    }
}
