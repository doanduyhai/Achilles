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

package info.archinnov.achilles.internals.config;

import info.archinnov.achilles.annotations.CompileTimeConfig;
import info.archinnov.achilles.type.CassandraVersion;
import info.archinnov.achilles.type.strategy.ColumnMappingStrategy;

@CompileTimeConfig(cassandraVersion = CassandraVersion.DSE_5_0_0,
        columnMappingStrategy = ColumnMappingStrategy.EXPLICIT,
        projectName = "IT_DSE_5_0_0")
public interface AchillesConfig {
}
