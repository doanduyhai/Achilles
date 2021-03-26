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

package info.archinnov.achilles.internals.cassandra_version;

import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.DSE_SEARCH;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.UDT;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import info.archinnov.achilles.internals.codegen.index.IndexSelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;

public class DSE_4_8_X extends V2_1 {

    public static DSE_4_8_X INSTANCE = new DSE_4_8_X();

    protected DSE_4_8_X() {
    }

    private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, DSE_SEARCH);

    @Override
    public String version() {
        return "DSE 4.8.X";
    }

    @Override
    public Set<CassandraFeature> getFeatures() {
        return SUPPORTED_FEATURES;
    }

    @Override
    public IndexSelectWhereDSLCodeGen indexSelectWhereDSLCodeGen() {
        return INDEX_SELECT_WHERE_DSL_CODE_GEN_DSE_4_8;
    }

    @Override
    public FieldValidator fieldValidator() {
        return FIELD_VALIDATOR_4_8;
    }
}
