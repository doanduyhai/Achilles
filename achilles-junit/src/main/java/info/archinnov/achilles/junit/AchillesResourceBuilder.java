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

import info.archinnov.achilles.junit.AchillesTestResource.Steps;

public class AchillesResourceBuilder {

	private Steps cleanupSteps = Steps.BOTH;
	private String[] tablesToCleanUp;
    private String keyspaceName;
	private String entityPackages;

	private AchillesResourceBuilder() {
	}

	private AchillesResourceBuilder(String entityPackages) {
		this.entityPackages = entityPackages;
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
	 * Start building an AchillesResource with no entity packages and default 'achilles_test' keyspace
	 */
	public static AchillesResource noEntityPackages() {
		return new AchillesResource(null,null);
	}

    /**
	 * Start building an AchillesResource with no entity packages and the provided keyspace name
	 */
	public static AchillesResource noEntityPackages(String keyspaceName) {
		return new AchillesResource(keyspaceName,null);
	}

    /**
     * Use provided keyspace instead of the default 'achilles_test' keyspace
     * @param keyspaceName
     *          keyspace name to be used
     */
    public AchillesResourceBuilder keyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
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
		return new AchillesResource(keyspaceName,entityPackages, cleanupSteps, tablesToCleanUp);
	}
}
