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

import java.util.Set;

import info.archinnov.achilles.internals.codegen.crud.CrudAPICodeGen;
import info.archinnov.achilles.internals.codegen.crud.cassandra2_1.CrudAPICodeGen2_1;
import info.archinnov.achilles.internals.codegen.crud.cassandra2_2.CrudAPICodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_1.DeleteDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_1.DeleteWhereDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra2_2.DeleteWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.delete.cassandra3_0.DeleteWhereDSLCodeGen3_0;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_1.SelectDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_1.SelectWhereDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2.SelectDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_2.SelectWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra3_10.SelectDSLCodeGen3_10;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra3_10.SelectWhereDSLCodeGen3_10;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra3_6.SelectWhereDSLCodeGen3_6;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_1.UpdateDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_1.UpdateWhereDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2.UpdateDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra2_2.UpdateWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra3_0.UpdateWhereDSLCodeGen3_0;
import info.archinnov.achilles.internals.codegen.dsl.update.cassandra3_6.UpdateDSLCodeGen3_6;
import info.archinnov.achilles.internals.codegen.function.FunctionParameterTypesCodeGen;
import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.codegen.function.cassandra2_1.FunctionParameterTypesCodeGen2_1;
import info.archinnov.achilles.internals.codegen.function.cassandra2_1.FunctionsRegistryCodeGen2_1;
import info.archinnov.achilles.internals.codegen.function.cassandra2_2.FunctionsRegistryCodeGen2_2;
import info.archinnov.achilles.internals.codegen.function.cassandra3_2.FunctionsRegistryCodeGen3_2;
import info.archinnov.achilles.internals.codegen.function.cassandra3_8.FunctionParameterTypesCodeGen3_8;
import info.archinnov.achilles.internals.codegen.index.IndexSelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.index.IndexSelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.index.cassandra2_1.IndexSelectDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.index.cassandra2_1.IndexSelectWhereDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.index.cassandra2_2.IndexSelectDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.index.cassandra2_2.IndexSelectWhereDSLCodeGen2_2;
import info.archinnov.achilles.internals.codegen.index.cassandra3_7.IndexSelectWhereDSLCodeGen3_7;
import info.archinnov.achilles.internals.codegen.index.dse_4_8.IndexSelectWhereDSLCodeGen_DSE_4_8;
import info.archinnov.achilles.internals.codegen.index.dse_5_0_0.IndexSelectWhereDSLCodeGen_DSE_5_0_0;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;
import info.archinnov.achilles.internals.parser.validator.NestedTypesValidator;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.BeanValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.FieldValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.NestedTypeValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.TypeValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra3_0.BeanValidator3_0;
import info.archinnov.achilles.internals.parser.validator.cassandra3_6.NestedTypeValidator3_6;
import info.archinnov.achilles.internals.parser.validator.cassandra3_7.FieldValidator3_7;
import info.archinnov.achilles.internals.parser.validator.dse_4_8.FieldValidator_DSE_4_8;

public interface InternalCassandraVersion {
    BeanValidator BEAN_VALIDATOR = new BeanValidator2_1();
    FieldValidator FIELD_VALIDATOR = new FieldValidator2_1();
    FieldValidator FIELD_VALIDATOR_3_7 = new FieldValidator3_7();
    FieldValidator FIELD_VALIDATOR_4_8 = new FieldValidator_DSE_4_8();
    TypeValidator TYPE_VALIDATOR = new TypeValidator2_1();
    NestedTypesValidator NESTED_TYPES_VALIDATOR = new NestedTypeValidator2_1();

    CrudAPICodeGen CRUD_API_CODE_GEN = new CrudAPICodeGen2_1();
    SelectDSLCodeGen SELECT_DSL_CODE_GEN = new SelectDSLCodeGen2_1();
    SelectWhereDSLCodeGen SELECT_WHERE_DSL_CODE_GEN = new SelectWhereDSLCodeGen2_1();

    IndexSelectDSLCodeGen INDEX_SELECT_DSL_CODE_GEN = new IndexSelectDSLCodeGen2_1();
    IndexSelectWhereDSLCodeGen INDEX_SELECT_WHERE_DSL_CODE_GEN = new IndexSelectWhereDSLCodeGen2_1();

    UpdateDSLCodeGen UPDATE_DSL_CODE_GEN = new UpdateDSLCodeGen2_1();
    UpdateWhereDSLCodeGen UPDATE_WHERE_DSL_CODE_GEN = new UpdateWhereDSLCodeGen2_1();

    DeleteDSLCodeGen DELETE_DSL_CODE_GEN = new DeleteDSLCodeGen2_1();
    DeleteWhereDSLCodeGen DELETE_WHERE_DSL_CODE_GEN = new DeleteWhereDSLCodeGen2_1();



    CrudAPICodeGen CRUD_API_CODE_GEN_2_2 = new CrudAPICodeGen2_2();
    SelectDSLCodeGen SELECT_DSL_CODE_GEN_2_2 = new SelectDSLCodeGen2_2();
    SelectDSLCodeGen SELECT_DSL_CODE_GEN_3_10 = new SelectDSLCodeGen3_10();
    IndexSelectDSLCodeGen INDEX_SELECT_DSL_CODE_GEN_2_2 = new IndexSelectDSLCodeGen2_2();
    SelectWhereDSLCodeGen SELECT_WHERE_DSL_CODE_GEN_2_2 = new SelectWhereDSLCodeGen2_2();
    IndexSelectWhereDSLCodeGen INDEX_SELECT_WHERE_DSL_CODE_GEN_2_2 = new IndexSelectWhereDSLCodeGen2_2();
    SelectWhereDSLCodeGen SELECT_WHERE_DSL_CODE_GEN_3_6 = new SelectWhereDSLCodeGen3_6();
    SelectWhereDSLCodeGen SELECT_WHERE_DSL_CODE_GEN_3_10 = new SelectWhereDSLCodeGen3_10();
    IndexSelectWhereDSLCodeGen INDEX_SELECT_WHERE_DSL_CODE_GEN_3_7 = new IndexSelectWhereDSLCodeGen3_7();
    IndexSelectWhereDSLCodeGen INDEX_SELECT_WHERE_DSL_CODE_GEN_DSE_4_8 = new IndexSelectWhereDSLCodeGen_DSE_4_8();
    IndexSelectWhereDSLCodeGen INDEX_SELECT_WHERE_DSL_CODE_GEN_DSE_5_0_0 = new IndexSelectWhereDSLCodeGen_DSE_5_0_0();

    UpdateDSLCodeGen UPDATE_DSL_CODE_GEN_2_2 = new UpdateDSLCodeGen2_2();
    UpdateDSLCodeGen UPDATE_DSL_CODE_GEN_3_6 = new UpdateDSLCodeGen3_6();
    UpdateWhereDSLCodeGen UPDATE_WHERE_DSL_CODE_GEN_2_2 = new UpdateWhereDSLCodeGen2_2();
    UpdateWhereDSLCodeGen UPDATE_WHERE_DSL_CODE_GEN_3_0 = new UpdateWhereDSLCodeGen3_0();
    DeleteWhereDSLCodeGen DELETE_WHERE_DSL_CODE_GEN_2_2 = new DeleteWhereDSLCodeGen2_2();
    DeleteWhereDSLCodeGen DELETE_WHERE_DSL_CODE_GEN_3_0 = new DeleteWhereDSLCodeGen3_0();

    BeanValidator BEAN_VALIDATOR_3_0 = new BeanValidator3_0();
    NestedTypesValidator NESTED_TYPES_VALIDATOR_3_6 = new NestedTypeValidator3_6();

    // Function calls
    FunctionsRegistryCodeGen FUNCTIONS_REGISTRY_CODE_GEN = new FunctionsRegistryCodeGen2_1();
    FunctionsRegistryCodeGen FUNCTIONS_REGISTRY_CODE_GEN_2_2 = new FunctionsRegistryCodeGen2_2();
    FunctionsRegistryCodeGen FUNCTIONS_REGISTRY_CODE_GEN_3_2 = new FunctionsRegistryCodeGen3_2();

    FunctionParameterTypesCodeGen FUNCTION_PARAMETER_TYPES_CODE_GEN = new FunctionParameterTypesCodeGen2_1();
    FunctionParameterTypesCodeGen FUNCTION_PARAMETER_TYPES_CODE_GEN_3_8 = new FunctionParameterTypesCodeGen3_8();

    Set<CassandraFeature> getFeatures();

    String version();

    default boolean supportsFeature(CassandraFeature feature) {
        return getFeatures().contains(feature);
    }

    default BeanValidator beanValidator() {
        return BEAN_VALIDATOR;
    }

    default FieldValidator fieldValidator() {
        return FIELD_VALIDATOR;
    }

    default TypeValidator typeValidator() {
        return TYPE_VALIDATOR;
    }

    default NestedTypesValidator nestedTypesValidator() {
        return NESTED_TYPES_VALIDATOR;
    }

    default CrudAPICodeGen crudApiCodeGen() {
        return CRUD_API_CODE_GEN;
    }

    default SelectDSLCodeGen selectDslCodeGen() {
        return SELECT_DSL_CODE_GEN;
    }

    default IndexSelectDSLCodeGen indexSelectDslCodeGen() {
        return INDEX_SELECT_DSL_CODE_GEN;
    }

    default SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
        return SELECT_WHERE_DSL_CODE_GEN;
    }

    default IndexSelectWhereDSLCodeGen indexSelectWhereDSLCodeGen() {
        return INDEX_SELECT_WHERE_DSL_CODE_GEN;
    }

    default UpdateDSLCodeGen updateDslCodeGen() {
        return UPDATE_DSL_CODE_GEN;
    }

    default UpdateWhereDSLCodeGen updateWhereDslCodeGen() {
        return UPDATE_WHERE_DSL_CODE_GEN;
    }

    default DeleteDSLCodeGen deleteDslCodeGen() {
        return DELETE_DSL_CODE_GEN;
    }

    default DeleteWhereDSLCodeGen deleteWhereDslCodeGen() {
        return DELETE_WHERE_DSL_CODE_GEN;
    }

    default FunctionsRegistryCodeGen functionsRegistryCodeGen() {
        return FUNCTIONS_REGISTRY_CODE_GEN;
    }

    default FunctionParameterTypesCodeGen functionParameterTypesCodeGen() {
        return FUNCTION_PARAMETER_TYPES_CODE_GEN;
    }
}
