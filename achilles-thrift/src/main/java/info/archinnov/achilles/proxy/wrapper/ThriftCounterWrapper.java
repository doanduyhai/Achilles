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

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.validation.Validator;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftCounterWrapper implements Counter {
	private static final Logger log = LoggerFactory.getLogger(ThriftCounterWrapper.class);

	private Object key;
	private Composite columnName;
	private ThriftAbstractDao counterDao;
	private ThriftPersistenceContext context;
	private ConsistencyLevel consistencyLevel;

	public ThriftCounterWrapper(ThriftPersistenceContext context) {
		this.context = context;
	}

	@Override
	public Long get() {
		log.trace("Get counter value for property {} of entity {}", columnName.get(0, STRING_SRZ), context
				.getEntityClass().getCanonicalName());
		return context.executeWithReadConsistencyLevel(new SafeExecutionContext<Long>() {
			@Override
			public Long execute() {
				return counterDao.getCounterValue(key, columnName);
			}
		}, consistencyLevel);
	}

	@Override
	public Long get(ConsistencyLevel readLevel) {
		Validator.validateNotNull(readLevel, "Read consistency level for counter get should not be null");

		log.trace("Get counter value for property {} of entity {} with consistency {}", columnName.get(0, STRING_SRZ),
				context.getEntityClass().getCanonicalName(), readLevel.name());

		return context.executeWithReadConsistencyLevel(new SafeExecutionContext<Long>() {
			@Override
			public Long execute() {
				return counterDao.getCounterValue(key, columnName);
			}
		}, readLevel);
	}

	@Override
	public void incr() {
		log.trace("Increment counter value for property {} of entity {}", columnName.get(0, STRING_SRZ), context
				.getEntityClass().getCanonicalName());

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.incrementCounter(key, columnName, 1L);
				return null;
			}
		}, consistencyLevel);

	}

	public void incr(ConsistencyLevel writeLevel) {
		Validator.validateNotNull(consistencyLevel, "Write consistency level for counter incr should not be null");

		log.trace("Increment counter value for property {} of entity {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(), writeLevel);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.incrementCounter(key, columnName, 1L);
				return null;
			}
		}, writeLevel);

	}

	@Override
	public void incr(final Long increment) {
		log.trace("Increment counter value for property {} of entity {} of {}", columnName.get(0, STRING_SRZ), context
				.getEntityClass().getCanonicalName(), increment);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.incrementCounter(key, columnName, increment);
				return null;
			}
		}, consistencyLevel);
	}

	@Override
	public void incr(final Long increment, ConsistencyLevel writeLevel) {
		Validator.validateNotNull(consistencyLevel, "Write consistency level for counter incr should not be null");

		log.trace("Increment counter value for property {} of entity {} of {}  with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(), increment, writeLevel);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.incrementCounter(key, columnName, increment);
				return null;
			}
		}, writeLevel);
	}

	@Override
	public void decr() {
		log.trace("Decrement counter value for property {} of entity {}", columnName.get(0, STRING_SRZ), context
				.getEntityClass().getCanonicalName());

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.decrementCounter(key, columnName, 1L);
				return null;
			}
		}, consistencyLevel);

	}

	@Override
	public void decr(ConsistencyLevel writeLevel) {
		Validator.validateNotNull(consistencyLevel, "Write consistency level for counter decr should not be null");

		log.trace("Decrement counter value for property {} of entity {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(), writeLevel);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.decrementCounter(key, columnName, 1L);
				return null;
			}
		}, writeLevel);
	}

	@Override
	public void decr(final Long decrement) {
		log.trace("Decrement counter value for property {} of entity {} of {}", columnName.get(0, STRING_SRZ), context
				.getEntityClass().getCanonicalName(), decrement);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.decrementCounter(key, columnName, decrement);
				return null;
			}
		}, consistencyLevel);
	}

	@Override
	public void decr(final Long decrement, ConsistencyLevel writeLevel) {
		Validator.validateNotNull(consistencyLevel, "Write consistency level for counter decr should not be null");

		log.trace("Decrement counter value for property {} of entity {} pof {} with consistency {}",
				columnName.get(0, STRING_SRZ), context.getEntityClass().getCanonicalName(), decrement, writeLevel);

		context.executeWithWriteConsistencyLevel(new SafeExecutionContext<Void>() {
			@Override
			public Void execute() {
				counterDao.decrementCounter(key, columnName, decrement);
				return null;
			}
		}, writeLevel);
	}

	public void setCounterDao(ThriftAbstractDao counterDao) {
		this.counterDao = counterDao;
	}

	public void setColumnName(Composite columnName) {
		this.columnName = columnName;
	}

	public void setConsistencyLevel(ConsistencyLevel readLevel) {
		this.consistencyLevel = readLevel;
	}

	public void setKey(Object key) {
		this.key = key;
	}
}
