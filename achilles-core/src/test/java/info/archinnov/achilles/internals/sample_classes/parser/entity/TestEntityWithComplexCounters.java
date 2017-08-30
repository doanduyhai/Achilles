/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

package info.archinnov.achilles.internals.sample_classes.parser.entity;

import java.util.UUID;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.internals.sample_classes.codecs.StringToLongCodec;

@APUnitTest
@Table
public class TestEntityWithComplexCounters {

    @PartitionKey
    private Long id;

    @Column
    @Static
    @Counter
    private Long staticCounter;

    @ClusteringColumn
    private UUID uuid;

    @Column
    @Counter
    private long simpleCounter;

    @Column
    @Counter
    @Codec(StringToLongCodec.class)
    private String counterWithCodec;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getStaticCounter() {
        return staticCounter;
    }

    public void setStaticCounter(Long staticCounter) {
        this.staticCounter = staticCounter;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public long getSimpleCounter() {
        return simpleCounter;
    }

    public void setSimpleCounter(long simpleCounter) {
        this.simpleCounter = simpleCounter;
    }

    public String getCounterWithCodec() {
        return counterWithCodec;
    }

    public void setCounterWithCodec(String counterWithCodec) {
        this.counterWithCodec = counterWithCodec;
    }
}
