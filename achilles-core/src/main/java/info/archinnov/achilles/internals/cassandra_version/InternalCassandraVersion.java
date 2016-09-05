/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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
import info.archinnov.achilles.internals.codegen.crud.cassandra2_2.CrudAPICodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_2.DeleteWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra3_0.DeleteWhereDSLCodeGen3_0;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2.SelectDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2.SelectWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2.UpdateDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2.UpdateWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra3_0.UpdateWhereDSLCodeGen3_0;
import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.codegen.function.cassandra2_2.FunctionsRegistryCodeGen2_2;
import info.archinnov.achilles.internals.codegen.function.cassandra3_2.FunctionsRegistryCodeGen3_2;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.parser.validator.cassandra_3_0.BeanValidator3_0;

public enum  InternalCassandraVersion implements BaseCassandraVersion {

    V2_1 {
        private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT);

        @Override
        public boolean supportsFeature(CassandraFeature feature) {
            return SUPPORTED_FEATURES.contains(feature);
        }
    },
    V2_2 {
        private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA);

        @Override
        public boolean supportsFeature(CassandraFeature feature) {
            return SUPPORTED_FEATURES.contains(feature);
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
        public SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
            return SELECT_WHERE_DSL_CODE_GEN_2_2;
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
    },
    V3_0 {
        private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, MATERIALIZED_VIEW);

        @Override
        public BeanValidator beanValidator() {
            return BEAN_VALIDATOR_3_0;
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
        public SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
            return SELECT_WHERE_DSL_CODE_GEN_2_2;
        }

        @Override
        public UpdateDSLCodeGen updateDslCodeGen() {
            return UPDATE_DSL_CODE_GEN_2_2;
        }

        @Override
        public UpdateWhereDSLCodeGen updateWhereDslCodeGen() {
            return UPDATE_WHERE_DSL_CODE_GEN_3_0;
        }

        @Override
        public DeleteWhereDSLCodeGen deleteWhereDslCodeGen() {
            return DELETE_WHERE_DSL_CODE_GEN_3_0;
        }

        @Override
        public FunctionsRegistryCodeGen functionsRegistryCodeGen() {
            return FUNCTIONS_REGISTRY_CODE_GEN_2_2;
        }

        @Override
        public boolean supportsFeature(CassandraFeature feature) {
            return SUPPORTED_FEATURES.contains(feature);
        }
    },
    V3_2 {
        private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, MATERIALIZED_VIEW);

        @Override
        public BeanValidator beanValidator() {
            return BEAN_VALIDATOR_3_0;
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
        public SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
            return SELECT_WHERE_DSL_CODE_GEN_2_2;
        }

        @Override
        public UpdateDSLCodeGen updateDslCodeGen() {
            return UPDATE_DSL_CODE_GEN_2_2;
        }

        @Override
        public UpdateWhereDSLCodeGen updateWhereDslCodeGen() {
            return UPDATE_WHERE_DSL_CODE_GEN_3_0;
        }

        @Override
        public DeleteWhereDSLCodeGen deleteWhereDslCodeGen() {
            return DELETE_WHERE_DSL_CODE_GEN_3_0;
        }

        @Override
        public FunctionsRegistryCodeGen functionsRegistryCodeGen() {
            return FUNCTIONS_REGISTRY_CODE_GEN_3_2;
        }

        @Override
        public boolean supportsFeature(CassandraFeature feature) {
            return SUPPORTED_FEATURES.contains(feature);
        }
    };

    private static final CrudAPICodeGen CRUD_API_CODE_GEN_2_2 = new CrudAPICodeGen2_2();
    private static final SelectDSLCodeGen SELECT_DSL_CODE_GEN_2_2 = new SelectDSLCodeGen2_2();
    private static final SelectWhereDSLCodeGen SELECT_WHERE_DSL_CODE_GEN_2_2 = new SelectWhereDSLCodeGen2_2();
    private static final UpdateDSLCodeGen UPDATE_DSL_CODE_GEN_2_2 = new UpdateDSLCodeGen2_2();
    private static final UpdateWhereDSLCodeGen UPDATE_WHERE_DSL_CODE_GEN_2_2 = new UpdateWhereDSLCodeGen2_2();
    private static final UpdateWhereDSLCodeGen UPDATE_WHERE_DSL_CODE_GEN_3_0 = new UpdateWhereDSLCodeGen3_0();
    private static final DeleteWhereDSLCodeGen DELETE_WHERE_DSL_CODE_GEN_2_2 = new DeleteWhereDSLCodeGen2_2();
    private static final DeleteWhereDSLCodeGen DELETE_WHERE_DSL_CODE_GEN_3_0 = new DeleteWhereDSLCodeGen3_0();

    private static final BeanValidator BEAN_VALIDATOR_3_0 = new BeanValidator3_0();
    private static final FunctionsRegistryCodeGen FUNCTIONS_REGISTRY_CODE_GEN_2_2 = new FunctionsRegistryCodeGen2_2();
    private static final FunctionsRegistryCodeGen FUNCTIONS_REGISTRY_CODE_GEN_3_2 = new FunctionsRegistryCodeGen3_2();

    public abstract boolean supportsFeature(CassandraFeature feature);

}
