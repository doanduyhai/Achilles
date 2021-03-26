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

import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;

public class V3_0 extends V2_2 {

    private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, JSON, MATERIALIZED_VIEW);

    public static V3_0 INSTANCE = new V3_0();

    protected V3_0() {
    }

    @Override
    public String version() {
        return "3.0.X";
    }

    @Override
    public Set<CassandraFeature> getFeatures() {
        return SUPPORTED_FEATURES;
    }

    @Override
    public BeanValidator beanValidator() {
        return BEAN_VALIDATOR_3_0;
    }

    @Override
    public UpdateWhereDSLCodeGen updateWhereDslCodeGen() {
        return UPDATE_WHERE_DSL_CODE_GEN_3_0;
    }

    @Override
    public DeleteWhereDSLCodeGen deleteWhereDslCodeGen() {
        return DELETE_WHERE_DSL_CODE_GEN_3_0;
    }

}
