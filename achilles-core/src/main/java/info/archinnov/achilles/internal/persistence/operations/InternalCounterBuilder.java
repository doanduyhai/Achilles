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

package info.archinnov.achilles.internal.persistence.operations;

import info.archinnov.achilles.type.Counter;

/**
 * <strong>Class internal to Achilles, DO NOT USE</strong>
 */
public class InternalCounterBuilder {
	public static Counter incr() {
		return new InternalCounterImpl(1L);
	}

	public static Counter incr(long incr) {
		return new InternalCounterImpl(incr);
	}

	public static Counter decr() {
		return new InternalCounterImpl(-1L);
	}

	public static Counter decr(long decr) {
		return new InternalCounterImpl(-1L * decr);
	}

	public static Counter initialValue(Long initialValue) {
		return new InternalCounterImpl(null, initialValue);
	}
}
