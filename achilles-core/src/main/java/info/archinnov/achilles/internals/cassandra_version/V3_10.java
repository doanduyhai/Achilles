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

import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.*;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;
import info.archinnov.achilles.internals.parser.validator.cassandra3_10.FieldValidator3_10;
import info.archinnov.achilles.internals.parser.validator.cassandra3_10.TypeValidator3_10;

public class V3_10 extends V3_9 {

    private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, JSON, MATERIALIZED_VIEW, SASI_INDEX, LITERAL_VALUES_IN_FUNCTION_CALL, DURATION_TYPE);

    public static V3_10 INSTANCE = new V3_10();

    protected V3_10() {
    }

    @Override
    public String version() {
        return "3.10";
    }

    @Override
    public Set<CassandraFeature> getFeatures() {
        return SUPPORTED_FEATURES;
    }

    @Override
    public FieldValidator fieldValidator() {
        return new FieldValidator3_10();
    }

    @Override
    public TypeValidator typeValidator() {
        return new TypeValidator3_10();
    }

    @Override
    public SelectDSLCodeGen selectDslCodeGen() {
        return SELECT_DSL_CODE_GEN_3_10;
    }

    @Override
    public SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
        return SELECT_WHERE_DSL_CODE_GEN_3_10;
    }
}
