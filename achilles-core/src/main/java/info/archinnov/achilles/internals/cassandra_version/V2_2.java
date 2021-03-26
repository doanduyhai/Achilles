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

import info.archinnov.achilles.internals.codegen.crud.CrudAPICodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.codegen.index.IndexSelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.index.IndexSelectWhereDSLCodeGen;

public class V2_2 extends V2_1 {

    private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, JSON);

    public static V2_2 INSTANCE = new V2_2();

    protected V2_2() {
    }

    @Override
    public String version() {
        return "2.2.X";
    }

    @Override
    public Set<CassandraFeature> getFeatures() {
        return SUPPORTED_FEATURES;
    }

    @Override
    public CrudAPICodeGen crudApiCodeGen() {
        return CRUD_API_CODE_GEN_2_2;
    }

    @Override
    public SelectDSLCodeGen selectDslCodeGen() {
        return SELECT_DSL_CODE_GEN_2_2;
    }

    @Override
    public IndexSelectDSLCodeGen indexSelectDslCodeGen() {
        return INDEX_SELECT_DSL_CODE_GEN_2_2;
    }

    @Override
    public SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
        return SELECT_WHERE_DSL_CODE_GEN_2_2;
    }

    @Override
    public IndexSelectWhereDSLCodeGen indexSelectWhereDSLCodeGen() {
        return INDEX_SELECT_WHERE_DSL_CODE_GEN_2_2;
    }

    @Override
    public UpdateDSLCodeGen updateDslCodeGen() {
        return UPDATE_DSL_CODE_GEN_2_2;
    }

    @Override
    public UpdateWhereDSLCodeGen updateWhereDslCodeGen() {
        return UPDATE_WHERE_DSL_CODE_GEN_2_2;
    }

    @Override
    public DeleteWhereDSLCodeGen deleteWhereDslCodeGen() {
        return DELETE_WHERE_DSL_CODE_GEN_2_2;
    }

    @Override
    public FunctionsRegistryCodeGen functionsRegistryCodeGen() {
        return FUNCTIONS_REGISTRY_CODE_GEN_2_2;
    }
}
