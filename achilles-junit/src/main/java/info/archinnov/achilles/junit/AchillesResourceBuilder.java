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

package info.archinnov.achilles.junit;

import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.utils.ConfigMap;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.type.TypedMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static info.archinnov.achilles.configuration.ConfigurationParameters.BEAN_VALIDATION_ENABLE;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITIES_LIST;
import static info.archinnov.achilles.configuration.ConfigurationParameters.ENTITY_PACKAGES;
import static info.archinnov.achilles.configuration.ConfigurationParameters.EVENT_INTERCEPTORS;
import static info.archinnov.achilles.configuration.ConfigurationParameters.KEYSPACE_NAME;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SCRIPT_LOCATIONS;

public class AchillesResourceBuilder {

	private Steps cleanupSteps = Steps.BOTH;
	private String[] tablesToCleanUp;
    private ConfigMap configMap = new ConfigMap();
    private TypedMap cassandraParams = new TypedMap();
    private List<String> scriptLocations = new ArrayList<>();

	private AchillesResourceBuilder() {
	}

	private AchillesResourceBuilder(String entityPackages) {
		configMap.put(ENTITY_PACKAGES, entityPackages);
	}

    private AchillesResourceBuilder(List<Class<?>> entityClasses) {
        configMap.put(ENTITIES_LIST, entityClasses);
    }

    /**
	 * Start building an AchillesResource with entity packages
	 * 
	 * @param entityPackages
	 *            packages to scan for entity discovery, comma separated
	 */
	public static AchillesResourceBuilder withEntityPackages(String entityPackages) {
		return new AchillesResourceBuilder(entityPackages);
	}

    /**
     * Start building an AchillesResource with list of entity classes
     *
     * @param entityClasses
     *            list of entity classes to manage
     */
    public static AchillesResourceBuilder withEntityClasses(List<Class<?>> entityClasses) {
        return new AchillesResourceBuilder(entityClasses);
    }

    /**
     * Start building an AchillesResource with list of entity classes
     *
     * @param entityClasses
     *            list of entity classes to manage
     */
    public static AchillesResourceBuilder withEntityClasses(Class<?> ... entityClasses) {
        return new AchillesResourceBuilder(Arrays.asList(entityClasses));
    }

	/**
	 * Start building an AchillesResource with no entity packages and default 'achilles_test' keyspace
	 */
	public static AchillesResourceBuilder noEntityPackages() {
		return new AchillesResourceBuilder();
	}

    /**
	 * Start building an AchillesResource with no entity packages and the provided keyspace name
	 */
	public static AchillesResourceBuilder noEntityPackages(String keyspaceName) {
        final AchillesResourceBuilder builder = new AchillesResourceBuilder();
        builder.configMap.put(KEYSPACE_NAME, keyspaceName);
        return builder;
	}

    /**
     * Use provided keyspace instead of the default 'achilles_test' keyspace
     * @param keyspaceName
     *          keyspace name to be used
     */
    public AchillesResourceBuilder withKeyspaceName(String keyspaceName) {
        configMap.put(KEYSPACE_NAME, keyspaceName);
        return this;
    }

    /**
     /**
     * Load an CQL script in the class path and execute it upon initialization
     * of the embedded Cassandra server
     *
     * <br/>
     *
     * Call this method as many times as there are CQL scripts to be executed.
     * <br/>
     * Example:
     * <br/>
     *  <pre class="code"><code class="java">
     *      AchillesResourceBuilder
     *          .withScript("script1.cql")
     *          .withScript("script2.cql")
     *          ...
     *  </code></pre>
     *
     * @param scriptLocation location of the CQL script in the <strong>class path</strong>
     */
    public AchillesResourceBuilder withScript(String scriptLocation) {
        scriptLocations.add(scriptLocation);
        return this;
    }


    /**
     * Activate Bean Validation. Do not forget to have a JAR with the real Implementation of Bean Validation in your dependencies
     */
    public AchillesResourceBuilder withBeanValidation() {
        configMap.put(BEAN_VALIDATION_ENABLE, true);
        return this;
    }

    /**
     * Register provided list of event interceptors
     * @param interceptors
     *          list of interceptors to register to Achilles
     */
    public AchillesResourceBuilder withInterceptors(List<Interceptor<?>> interceptors) {
        configMap.put(EVENT_INTERCEPTORS, interceptors);
        return this;
    }


    /**
     * Provide map of Achilles configuration
     * @param achillesConfig
     *          map of all Achilles configuration
     */
    public AchillesResourceBuilder withAchillesConfig(Map<ConfigurationParameters, Object> achillesConfig) {
        configMap.putAll(achillesConfig);
        return this;
    }


	/**
	 * Tables to be truncated during unit tests
	 * 
	 * @param tablesToCleanUp
	 *            list of tables to truncate before and/or after tests
	 */
	public AchillesResourceBuilder tablesToTruncate(String... tablesToCleanUp) {
		this.tablesToCleanUp = tablesToCleanUp;
		return this;
	}

	/**
	 * Truncate tables BEFORE each test
	 */
	public AchillesResourceBuilder truncateBeforeTest() {
		this.cleanupSteps = Steps.BEFORE_TEST;
		return this;
	}

	/**
	 * Truncate tables AFTER each test
	 */
	public AchillesResourceBuilder truncateAfterTest() {
		this.cleanupSteps = Steps.AFTER_TEST;
		return this;
	}

	/**
	 * Truncate tables BEFORE and AFTER each test
	 */
	public AchillesResourceBuilder truncateBeforeAndAfterTest() {
		this.cleanupSteps = Steps.BOTH;
		return this;
	}

	public AchillesResource build() {
        final TypedMap cassandraParams = buildCassandraParams();
        return new AchillesResource(cassandraParams, configMap, cleanupSteps, tablesToCleanUp);
	}

    private TypedMap buildCassandraParams() {
        cassandraParams.put(SCRIPT_LOCATIONS, scriptLocations);
        return cassandraParams;
    }
}
