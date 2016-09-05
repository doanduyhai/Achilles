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

package info.archinnov.achilles.type;

/**
 * Define which version of Cassandra you want Achilles to generate source code against
 */
public enum CassandraVersion {


    CASSANDRA_2_1_X,
    /**
     * New features:
     * <br/>
     * <ul>
     *     <li>User Defined Function, User Defined Aggregate</li>
     *     <li>JSON Syntax</li>
     * </ul>
     */
    CASSANDRA_2_2_X,
    /**
     * New features:
     * <br/>
     * <ul>
     *     <li>Materialized Views</li>
     *     <li>Allow custom indexes with 0,1 or multiple target columns (CASSANDRA-10124)</li>
     *     <li>Support for IN restrictions on any partition key component or clustering key
     *          as well as support for EQ and IN multicolumn restrictions has been added to
     *          UPDATE and DELETE statement</li>
     *     <li>Support for single-column and multi-column slice restrictions (>, >=, <= and <)
     *          has been added to DELETE statements</li>
     * </ul>
     */
    CASSANDRA_3_0_X,
    CASSANDRA_3_1,
    /**
     * New features:
     * <br/>
     * <ul>
     *     <li>Add support for type casting in selection clause (CASSANDRA-10310)</li>
     * </ul>
     */
    CASSANDRA_3_2,
    CASSANDRA_3_3,
    CASSANDRA_3_4,
    CASSANDRA_3_5,
    /**
     * New features:
     * <br/>
     * <ul>
     *     <li>Add static column support to SASI index (CASSANDRA-11183)</li>
     *     <li>Allow per-partition LIMIT clause in CQL (CASSANDRA-7017)</li>
     *     <li>Support for non-frozen user-defined types, updating individual fields of user-defined types (CASSANDRA-7423)</li>
     * </ul>
     *
     */
    CASSANDRA_3_6,
    /**
     * New features:
     * <br/>
     * <ul>
     *     <li><strong>Stable</strong> SASI index with Support LIKE operator in prepared statements (CASSANDRA-11456)</li>
     * </ul>
     *
     */
    CASSANDRA_3_7,
    /**
     * New features:
     * <br/>
     * <ul>
     *     <li>Allow literal values in UDF/UDA parameters (CASSANDRA-10783)</li>
     * </ul>
     *
     */
    CASSANDRA_3_9
}
