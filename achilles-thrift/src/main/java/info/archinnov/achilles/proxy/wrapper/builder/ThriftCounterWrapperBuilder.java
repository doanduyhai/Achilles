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
package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;

public class ThriftCounterWrapperBuilder {
	private Object key;
	private Composite columnName;
	private ThriftAbstractDao counterDao;
	private ThriftPersistenceContext context;
	private ConsistencyLevel consistencyLevel;

	public static ThriftCounterWrapperBuilder builder(ThriftPersistenceContext context) {
		return new ThriftCounterWrapperBuilder(context);
	}

	protected ThriftCounterWrapperBuilder(ThriftPersistenceContext context) {
		this.context = context;
	}

	public ThriftCounterWrapperBuilder key(Object key) {
		this.key = key;
		return this;
	}

	public ThriftCounterWrapperBuilder counterDao(ThriftAbstractDao counterDao) {
		this.counterDao = counterDao;
		return this;
	}

	public ThriftCounterWrapperBuilder columnName(Composite columnName) {
		this.columnName = columnName;
		return this;
	}

	public ThriftCounterWrapperBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	public ThriftCounterWrapper build() {
		ThriftCounterWrapper wrapper = new ThriftCounterWrapper(context);
		wrapper.setCounterDao(counterDao);
		wrapper.setColumnName(columnName);
		wrapper.setConsistencyLevel(consistencyLevel);
		wrapper.setKey(key);
		return wrapper;
	}
}
