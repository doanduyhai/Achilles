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

import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.MATERIALIZED_VIEW;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.UDF_UDA;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.UDT;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

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
    },
    V3_0 {
        private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT, UDF_UDA, MATERIALIZED_VIEW);

        @Override
        public BeanValidator beanValidator() {
            return new BeanValidator3_0();
        }

        @Override
        public boolean supportsFeature(CassandraFeature feature) {
            return SUPPORTED_FEATURES.contains(feature);
        }
    };

    public abstract boolean supportsFeature(CassandraFeature feature);

}
