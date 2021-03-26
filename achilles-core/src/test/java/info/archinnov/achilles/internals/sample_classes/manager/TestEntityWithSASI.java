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

package info.archinnov.achilles.internals.sample_classes.manager;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.SASI;
import info.archinnov.achilles.annotations.Table;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@Table(keyspace = "manager_codegen", table = "entity_with_sasi")
public class TestEntityWithSASI {

    @PartitionKey
    private Long id;

    @SASI(indexMode = SASI.IndexMode.SPARSE)
    @Column
    private int sparse;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getSparse() {
        return sparse;
    }

    public void setSparse(int sparse) {
        this.sparse = sparse;
    }
}
