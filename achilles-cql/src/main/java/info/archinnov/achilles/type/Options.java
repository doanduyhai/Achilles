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
package info.archinnov.achilles.type;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class Options {

	ConsistencyLevel consistency;

	Integer ttl;

	Long timestamp;

	Options() {
	}

	public Optional<ConsistencyLevel> getConsistencyLevel() {
		return Optional.fromNullable(consistency);
	}

	public Optional<Integer> getTtl() {
		return Optional.fromNullable(ttl);
	}

	public Optional<Long> getTimestamp() {
		return Optional.fromNullable(timestamp);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(Options.class)
            .add("Consistency Level",this.consistency)
            .add("Time to live", this.ttl)
            .add("Timestamp",this.timestamp)
            .toString();
	}

	public Options duplicateWithoutTtlAndTimestamp() {
		return OptionsBuilder.withConsistency(consistency);
	}

	public Options duplicateWithNewConsistencyLevel(ConsistencyLevel consistencyLevel) {
		return OptionsBuilder.withConsistency(consistencyLevel).withTtl(ttl).withTimestamp(timestamp);
	}
}
