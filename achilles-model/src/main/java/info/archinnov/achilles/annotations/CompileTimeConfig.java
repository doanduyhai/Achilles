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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;

import info.archinnov.achilles.type.CassandraVersion;
import info.archinnov.achilles.type.strategy.ColumnMappingStrategy;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

/**
 *
 *
 *
 *
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface CompileTimeConfig {

    InsertStrategy insertStrategy() default InsertStrategy.ALL_FIELDS;

    NamingStrategy namingStrategy() default NamingStrategy.LOWER_CASE;

    ColumnMappingStrategy columnMappingStrategy() default ColumnMappingStrategy.EXPLICIT;

    CassandraVersion cassandraVersion() default CassandraVersion.CASSANDRA_2_1_X;

    String projectName() default "";
}
