/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.counter;

/**
 * Interface defining the structure of internal Achilles Counter table
 *
 * This table creation script is
 *
 * <pre class="code"><code class="sql">
 *
 *   CREATE TABLE achilles_counter_table (
 *      fqcn text,
 *      primary_key text,
 *      property_name text,
 *      counter_value counter,
 *      PRIMARY KEY((fqcn,primary_key),property_name));
 *
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Counters#simple-counters-mapping" target="_blank">Simple counter support</a>
 */
public interface AchillesCounter {

	public static final String CQL_COUNTER_TABLE = "achilles_counter_table";
	public static final String CQL_COUNTER_FQCN = "fqcn";
	public static final String CQL_COUNTER_PRIMARY_KEY = "primary_key";
	public static final String CQL_COUNTER_PROPERTY_NAME = "property_name";
	public static final String CQL_COUNTER_VALUE = "counter_value";

	public static enum CQLQueryType {
		INCR, DECR, SELECT, DELETE;
	}

    public static enum ClusteredCounterStatement {
        SELECT_ALL,DELETE_ALL;
    }
}
