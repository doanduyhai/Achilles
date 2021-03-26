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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;

import info.archinnov.achilles.type.CassandraVersion;
import info.archinnov.achilles.type.strategy.ColumnMappingStrategy;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

/**
 *
 * Define options for source code generation at compile time.
 * <br/>
 * <br/>
 * With this annotation you can define:
 * <br/>
 * <ul>
 *     <li><em>insertStrategy()</em>: the insert strategy to use, default = {@link info.archinnov.achilles.type.strategy.InsertStrategy}.ALL_FIELDS.
 *     See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Insert-Strategy">insert strategy</a> for more details</li>
 *     <li><em>namingStrategy()</em>: the naming strategy to use, default = {@link info.archinnov.achilles.type.strategy.NamingStrategy}.LOWER_CASE.
 *     See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Table-Mapping#naming-strategy">naming strategy</a> for more details</li>
 *     <li><em>columnMappingStrategy()</em>: the column mapping strategy to use, default = {@link info.archinnov.achilles.type.strategy.ColumnMappingStrategy}.EXPLICIT.
 *     See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Table-Mapping#column-mapping-strategy">column mapping strategy</a> for more details</li>
 *     <li><em>cassandraVersion()</em>: the Cassandra version to use, default = {@link info.archinnov.achilles.type.CassandraVersion}.CASSANDRA_3_0_X.
 *     See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Compile-Time-Config"> for more details</li>
 *     <li><em>projectName()</em>: optionally the name of your project in the context of multi-project support.
 *     See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Multi-Project-Support"> for more details</li>
 * </ul>
 * <br/>
 * See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Compile-Time-Config">Configuring Achilles at compile time</a> for further details
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface CompileTimeConfig {

    /**
     * Define the insert strategy to use, default = {@link info.archinnov.achilles.type.strategy.InsertStrategy}.ALL_FIELDS.
     * See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Insert-Strategy">insert strategy</a> for more details
     */
    InsertStrategy insertStrategy() default InsertStrategy.ALL_FIELDS;

    /**
     * Define the naming strategy to use, default = {@link info.archinnov.achilles.type.strategy.NamingStrategy}.LOWER_CASE.
     * See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Table-Mapping#naming-strategy">naming strategy</a> for more details
     */
    NamingStrategy namingStrategy() default NamingStrategy.LOWER_CASE;

    /**
     * Define the column mapping strategy to use, default = {@link info.archinnov.achilles.type.strategy.ColumnMappingStrategy}.EXPLICIT.
     * See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Table-Mapping#column-mapping-strategy">column mapping strategy</a> for more details
     */
    ColumnMappingStrategy columnMappingStrategy() default ColumnMappingStrategy.EXPLICIT;

    /**
     * Define the Cassandra version to use, default = {@link info.archinnov.achilles.type.CassandraVersion}.CASSANDRA_3_0_X.
     * See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Compile-Time-Config"> for more details
     */
    CassandraVersion cassandraVersion() default CassandraVersion.CASSANDRA_3_0_X;

    /**
     * Define the name of your project in the context of multi-project support.
     * See <a target="_blank" href="https://github.com/doanduyhai/Achilles/wiki/Multi-Project-Support"> for more details
     */
    String projectName() default "";
}
