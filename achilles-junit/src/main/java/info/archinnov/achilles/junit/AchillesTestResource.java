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

import static info.archinnov.achilles.junit.AchillesTestResource.Steps.BOTH;

import org.junit.rules.ExternalResource;

public abstract class AchillesTestResource extends ExternalResource {

	protected final String[] tables;
	private Steps steps = BOTH;

	public AchillesTestResource(String... tables) {
		this.tables = tables;
	}

	public AchillesTestResource(Steps cleanUpSteps, String... tables) {
		this.steps = cleanUpSteps;
		this.tables = tables;
	}

	protected void before() throws Throwable {
		if (steps.isBefore())
			truncateTables();
	}

	protected void after() {
		if (steps.isAfter())
			truncateTables();
	}

	protected abstract void truncateTables();

	public static enum Steps {
		BEFORE_TEST, AFTER_TEST, BOTH;

		public boolean isBefore() {
			return (this == BOTH || this == BEFORE_TEST);
		}

		public boolean isAfter() {
			return (this == BOTH || this == AFTER_TEST);
		}
	}
}
