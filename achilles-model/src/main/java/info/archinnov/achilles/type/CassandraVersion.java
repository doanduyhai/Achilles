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
    CASSANDRA_2_2_X,
    CASSANDRA_3_0_X,
    CASSANDRA_3_1,
    CASSANDRA_3_2,
    CASSANDRA_3_3,
    CASSANDRA_3_4,
    CASSANDRA_3_5,
    CASSANDRA_3_6,
    CASSANDRA_3_7,
    CASSANDRA_3_9
}
