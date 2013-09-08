/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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
package info.archinnov.achilles.configuration;

public interface CQLConfigurationParameters {
	String CONNECTION_CONTACT_POINTS_PARAM = "achilles.cassandra.connection.contactPoints";
	String CONNECTION_PORT_PARAM = "achilles.cassandra.connection.port";
	String KEYSPACE_NAME_PARAM = "achilles.cassandra.keyspace.name";
	String COMPRESSION_TYPE = "achilles.cassandra.compression.type";
	String RETRY_POLICY = "achilles.cassandra.retry.policy";
	String LOAD_BALANCING_POLICY = "achilles.cassandra.load.balancing.policy";
	String RECONNECTION_POLICY = "achilles.cassandra.reconnection.policy";
	String USERNAME = "achilles.cassandra.username";
	String PASSWORD = "achilles.cassandra.password";
	String DISABLE_JMX = "achilles.cassandra.disable.jmx";
	String DISABLE_METRICS = "achilles.cassandra.disable.metrics";
	String SSL_ENABLED = "achilles.cassandra.ssl.enabled";
	String SSL_OPTIONS = "achilles.cassandra.ssl.options";
}
