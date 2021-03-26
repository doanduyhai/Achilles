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

import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.UDT;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class V2_1 implements InternalCassandraVersion {

    public static V2_1 INSTANCE = new V2_1();

    protected V2_1() {
    }

    private final Set<CassandraFeature> SUPPORTED_FEATURES = ImmutableSet.of(UDT);

    @Override
    public String version() {
        return "2.1.X";
    }

    @Override
    public Set<CassandraFeature> getFeatures() {
        return SUPPORTED_FEATURES;
    }
}
