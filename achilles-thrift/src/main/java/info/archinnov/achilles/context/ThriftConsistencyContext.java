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
package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.type.ConsistencyLevel;

public class ThriftConsistencyContext {
	private final AchillesConsistencyLevelPolicy policy;

	private ConsistencyLevel consistencyLevel;

	public ThriftConsistencyContext(AchillesConsistencyLevelPolicy policy, ConsistencyLevel consistencyLevel) {
		this.policy = policy;
		this.consistencyLevel = consistencyLevel;
	}

	public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context) {
		if (consistencyLevel != null) {
			policy.setCurrentReadLevel(consistencyLevel);
			return reinitConsistencyLevels(context);
		} else {
			return context.execute();
		}
	}

	public <T> T executeWithReadConsistencyLevel(SafeExecutionContext<T> context, ConsistencyLevel readLevel) {

		if (readLevel != null) {
			policy.setCurrentReadLevel(readLevel);
			return reinitConsistencyLevels(context);
		} else {
			return context.execute();
		}
	}

	public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context) {
		if (consistencyLevel != null) {
			policy.setCurrentWriteLevel(consistencyLevel);
			return reinitConsistencyLevels(context);
		} else {
			return context.execute();
		}
	}

	public <T> T executeWithWriteConsistencyLevel(SafeExecutionContext<T> context, ConsistencyLevel writeLevel) {

		if (writeLevel != null) {
			policy.setCurrentWriteLevel(writeLevel);
			return reinitConsistencyLevels(context);
		} else {
			return context.execute();
		}
	}

	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		if (consistencyLevel != null) {
			this.consistencyLevel = consistencyLevel;
			policy.setCurrentReadLevel(consistencyLevel);
		}
	}

	public void setConsistencyLevel() {
		if (consistencyLevel != null) {
			policy.setCurrentReadLevel(consistencyLevel);
		}
	}

	public void reinitConsistencyLevels() {
		policy.reinitCurrentConsistencyLevels();
		policy.reinitDefaultConsistencyLevels();
	}

	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	private <T> T reinitConsistencyLevels(SafeExecutionContext<T> context) {
		try {
			return context.execute();
		} finally {
			policy.reinitCurrentConsistencyLevels();
			policy.reinitDefaultConsistencyLevels();
		}
	}

}
