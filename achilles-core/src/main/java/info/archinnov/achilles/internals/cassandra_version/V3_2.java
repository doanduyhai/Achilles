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

import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;

public class V3_2 extends V3_0 {

    private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, JSON, MATERIALIZED_VIEW);

    public static V3_2 INSTANCE = new V3_2();

    protected V3_2() {
    }

    @Override
    public String version() {
        return "3.2";
    }

    @Override
    public Set<CassandraFeature> getFeatures() {
        return SUPPORTED_FEATURES;
    }

    @Override
    public FunctionsRegistryCodeGen functionsRegistryCodeGen() {
        return FUNCTIONS_REGISTRY_CODE_GEN_3_2;
    }


}
