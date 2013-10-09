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

package info.archinnov.achilles.junit;

import info.archinnov.achilles.junit.AchillesTestResource.Steps;

public class AchillesCQLResourceBuilder {

	private Steps cleanupSteps = Steps.BOTH;
	private String[] tablesToCleanUp;
	private String entityPackages;

	private AchillesCQLResourceBuilder() {
	}

	private AchillesCQLResourceBuilder(String entityPackages) {
		this.entityPackages = entityPackages;
	}

	public static AchillesCQLResourceBuilder withEntityPackages(String entityPackages) {
		return new AchillesCQLResourceBuilder(entityPackages);
	}

	public static AchillesCQLResource noEntityPackages() {
		return new AchillesCQLResource(null);
	}

	public AchillesCQLResourceBuilder tablesToTruncate(String... tablesToCleanUp) {
		this.tablesToCleanUp = tablesToCleanUp;
		return this;
	}

	public AchillesCQLResourceBuilder truncateBeforeTest() {
		this.cleanupSteps = Steps.BEFORE_TEST;
		return this;
	}

	public AchillesCQLResourceBuilder truncateAfterTest() {
		this.cleanupSteps = Steps.AFTER_TEST;
		return this;
	}

	public AchillesCQLResourceBuilder truncateBeforeAndAfterTest() {
		this.cleanupSteps = Steps.BOTH;
		return this;
	}

	public AchillesCQLResource build() {
		return new AchillesCQLResource(entityPackages, cleanupSteps, tablesToCleanUp);
	}
}
