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
package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

public class CounterBuilder {
	public static Counter incr() {
		return new CounterImpl(1L);
	}

	public static Counter incr(Long incr) {
		return new CounterImpl(incr);
	}

	public static Counter decr() {
		return new CounterImpl(-1L);
	}

	public static Counter decr(Long decr) {
		return new CounterImpl(-1L * decr);
	}

	public static class CounterImpl implements Counter {

		private final Long value;

		private CounterImpl(Long value) {
			this.value = value;
		}

		@Override
		public Long get() {
			return value;
		}

		@Override
		public Long get(ConsistencyLevel readLevel) {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void incr() {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void incr(Long increment) {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void decr() {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void decr(Long decrement) {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void incr(ConsistencyLevel writeLevel) {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void incr(Long increment, ConsistencyLevel writeLevel) {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void decr(ConsistencyLevel writeLevel) {
			throw new UnsupportedOperationException("This method is not meant to be called");
		}

		@Override
		public void decr(Long decrement, ConsistencyLevel writeLevel) {
			throw new UnsupportedOperationException("This method is not meant to be called");

		}
	}
}
