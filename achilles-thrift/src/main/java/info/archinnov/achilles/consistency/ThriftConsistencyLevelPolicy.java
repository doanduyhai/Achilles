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
package info.archinnov.achilles.consistency;

import static info.archinnov.achilles.consistency.ThriftConsistencyConvertor.getHectorLevel;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Map;

import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftConsistencyLevelPolicy extends
		AchillesConsistencyLevelPolicy implements ConsistencyLevelPolicy {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftConsistencyLevelPolicy.class);

	static final ThreadLocal<HConsistencyLevel> defaultReadConsistencyLevelTL = new ThreadLocal<HConsistencyLevel>();
	static final ThreadLocal<HConsistencyLevel> defaultWriteConsistencyLevelTL = new ThreadLocal<HConsistencyLevel>();
	static final ThreadLocal<ConsistencyLevel> currentReadConsistencyLevel = new ThreadLocal<ConsistencyLevel>();
	static final ThreadLocal<ConsistencyLevel> currentWriteConsistencyLevel = new ThreadLocal<ConsistencyLevel>();

	public ThriftConsistencyLevelPolicy(ConsistencyLevel defaultReadLevel,
			ConsistencyLevel defaultWriteLevel,
			Map<String, ConsistencyLevel> readConsistencyMap,
			Map<String, ConsistencyLevel> writeConsistencyMap) {
		super(defaultReadLevel, defaultWriteLevel, readConsistencyMap,
				writeConsistencyMap);

		log.debug(
				"Initializing Achilles Configurable Consistency Level Policy with default read/write levels {}/{} and read/write level maps {}/{}",
				defaultReadLevel, defaultWriteLevel, readConsistencyMap,
				writeConsistencyMap);

	}

	@Override
	public HConsistencyLevel get(OperationType op) {
		HConsistencyLevel result;
		HConsistencyLevel defaultRead = ThriftConsistencyConvertor
				.getHectorLevel(defaultGlobalReadConsistencyLevel);
		HConsistencyLevel defaultWrite = ThriftConsistencyConvertor
				.getHectorLevel(defaultGlobalWriteConsistencyLevel);

		switch (op) {
		case READ:
			result = defaultReadConsistencyLevelTL.get() != null ? defaultReadConsistencyLevelTL
					.get() : defaultRead;
			log.trace(
					"Set default read consistency level to {} in the thread {}",
					result.name(), Thread.currentThread());
			break;
		case WRITE:
			result = defaultWriteConsistencyLevelTL.get() != null ? defaultWriteConsistencyLevelTL
					.get() : defaultWrite;
			log.trace(
					"Set default write consistency level to {} in the thread {}",
					result.name(), Thread.currentThread());
			break;
		default:
			result = HConsistencyLevel.ONE;
		}

		return result;
	}

	@Override
	public HConsistencyLevel get(OperationType op, String columnFamily) {
		HConsistencyLevel result;
		switch (op) {
		case READ:
			result = defaultReadConsistencyLevelTL.get() != null ? defaultReadConsistencyLevelTL
					.get()
					: getHectorLevel(getConsistencyLevelForRead(columnFamily));
			log.trace(
					"Set default read consistency of column family {} level to {} in the thread {}",
					result.name(), columnFamily, Thread.currentThread());
			break;
		case WRITE:
			result = defaultWriteConsistencyLevelTL.get() != null ? defaultWriteConsistencyLevelTL
					.get()
					: getHectorLevel(getConsistencyLevelForWrite(columnFamily));
			log.trace(
					"Set default write consistency of column family {} level to {} in the thread {}",
					result.name(), columnFamily, Thread.currentThread());
			break;
		default:
			result = HConsistencyLevel.ONE;
		}
		return result;
	}

	public void loadConsistencyLevelForRead(String columnFamily) {
		ConsistencyLevel currentLevel = currentReadConsistencyLevel.get();
		if (currentLevel != null) {
			defaultReadConsistencyLevelTL.set(getHectorLevel(currentLevel));
			log.trace(
					"Load default read consistency of column family {} level to {} in the thread {}",
					currentLevel.name(), columnFamily, Thread.currentThread());
		} else {
			HConsistencyLevel level = this
					.get(OperationType.READ, columnFamily);
			defaultReadConsistencyLevelTL.set(level);
			log.trace(
					"Load default read consistency of column family {} level to {} in the thread {}",
					level.name(), columnFamily, Thread.currentThread());
		}
	}

	public void loadConsistencyLevelForWrite(String columnFamily) {
		ConsistencyLevel currentLevel = currentWriteConsistencyLevel.get();
		if (currentLevel != null) {
			defaultWriteConsistencyLevelTL.set(getHectorLevel(currentLevel));
			log.trace(
					"Load default write consistency of column family {} level to {} in the thread {}",
					currentLevel.name(), columnFamily, Thread.currentThread());
		} else {
			HConsistencyLevel level = this.get(OperationType.WRITE,
					columnFamily);
			defaultWriteConsistencyLevelTL.set(level);
			log.trace(
					"Load default write consistency of column family {} level to {} in the thread {}",
					level.name(), columnFamily, Thread.currentThread());
		}
	}

	public void reinitDefaultConsistencyLevels() {
		log.trace(
				"Reinit defaut read/write consistency levels in the thread {}",
				Thread.currentThread());
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
	}

	public void reinitCurrentConsistencyLevels() {
		log.trace(
				"Reinit current read/write consistency levels in the thread {}",
				Thread.currentThread());
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
	}

	public ConsistencyLevel getCurrentReadLevel() {
		log.trace("Current read consistency level is {} in the thread {}",
				currentReadConsistencyLevel.get(), Thread.currentThread());
		return currentReadConsistencyLevel.get();
	}

	public void setCurrentReadLevel(ConsistencyLevel readLevel) {
		log.trace("Set current read consistency level to {} in the thread {}",
				readLevel, Thread.currentThread());
		currentReadConsistencyLevel.set(readLevel);
	}

	public void removeCurrentReadLevel() {
		log.trace("Remove current read consistency level  in the thread {}",
				Thread.currentThread());
		currentReadConsistencyLevel.remove();
	}

	public ConsistencyLevel getCurrentWriteLevel() {
		log.trace("Current write consistency level is {} in the thread {}",
				currentReadConsistencyLevel.get(), Thread.currentThread());
		return currentWriteConsistencyLevel.get();
	}

	public void setCurrentWriteLevel(ConsistencyLevel writeLevel) {
		log.trace("Set current write consistency level to {} in the thread {}",
				writeLevel, Thread.currentThread());
		currentWriteConsistencyLevel.set(writeLevel);
	}

	public void removeCurrentWriteLevel() {
		log.trace("Remove current write consistency level  in the thread {}",
				Thread.currentThread());
		currentWriteConsistencyLevel.remove();
	}
}
