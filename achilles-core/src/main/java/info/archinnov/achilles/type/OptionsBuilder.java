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

public class OptionsBuilder {

	private static final NoOptions noOptions = new NoOptions();

	public static NoOptions noOptions() {
		return noOptions;
	}

	public static InternalOptionsBuilder withConsistency(ConsistencyLevel consistencyLevel) {
		return new InternalOptionsBuilder(consistencyLevel);
	}

	public static InternalOptionsBuilder withTtl(Integer ttl) {
		return new InternalOptionsBuilder(ttl);
	}

	public static InternalOptionsBuilder withTimestamp(Long timestamp) {
		return new InternalOptionsBuilder(timestamp);
	}

	public static class NoOptions extends Options {
		protected NoOptions() {
		}

		@Override
		public NoOptions duplicateWithoutTtlAndTimestamp() {
			return this;
		}
	}

	public static class InternalOptionsBuilder extends Options {
		protected InternalOptionsBuilder(ConsistencyLevel consistencyLevel) {
			super.consistency = consistencyLevel;
		}

		protected InternalOptionsBuilder(Integer ttl) {
			super.ttl = ttl;
		}

		protected InternalOptionsBuilder(Long timestamp) {
			super.timestamp = timestamp;
		}

		public InternalOptionsBuilder withConsistency(ConsistencyLevel consistencyLevel) {
			super.consistency = consistencyLevel;
			return this;
		}

		public InternalOptionsBuilder withTtl(Integer ttl) {
			super.ttl = ttl;
			return this;
		}

		public InternalOptionsBuilder withTimestamp(Long timestamp) {
			super.timestamp = timestamp;
			return this;
		}
	}

}
