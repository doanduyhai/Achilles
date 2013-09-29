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

import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Map;

public abstract class AchillesConsistencyLevelPolicy {

	protected ConsistencyLevel defaultGlobalReadConsistencyLevel;
	protected ConsistencyLevel defaultGlobalWriteConsistencyLevel;

	protected Map<String, ConsistencyLevel> readCfConsistencyLevels;
	protected Map<String, ConsistencyLevel> writeCfConsistencyLevels;

	protected AchillesConsistencyLevelPolicy(ConsistencyLevel defaultGlobalReadConsistencyLevel,
			ConsistencyLevel defaultGlobalWriteConsistencyLevel, Map<String, ConsistencyLevel> readCfConsistencyLevels,
			Map<String, ConsistencyLevel> writeCfConsistencyLevels) {
		this.defaultGlobalReadConsistencyLevel = defaultGlobalReadConsistencyLevel;
		this.defaultGlobalWriteConsistencyLevel = defaultGlobalWriteConsistencyLevel;
		this.readCfConsistencyLevels = readCfConsistencyLevels;
		this.writeCfConsistencyLevels = writeCfConsistencyLevels;
	}

	public ConsistencyLevel getConsistencyLevelForRead(String columnFamily) {
		return readCfConsistencyLevels.get(columnFamily) != null ? readCfConsistencyLevels.get(columnFamily)
				: defaultGlobalReadConsistencyLevel;
	}

	public void setConsistencyLevelForRead(ConsistencyLevel consistencyLevel, String columnFamily) {
		readCfConsistencyLevels.put(columnFamily, consistencyLevel);
	}

	public ConsistencyLevel getConsistencyLevelForWrite(String columnFamily) {
		return writeCfConsistencyLevels.get(columnFamily) != null ? writeCfConsistencyLevels.get(columnFamily)
				: defaultGlobalWriteConsistencyLevel;
	}

	public void setConsistencyLevelForWrite(ConsistencyLevel consistencyLevel, String columnFamily) {
		writeCfConsistencyLevels.put(columnFamily, consistencyLevel);
	}

	public ConsistencyLevel getDefaultGlobalReadConsistencyLevel() {
		return defaultGlobalReadConsistencyLevel;
	}

	public ConsistencyLevel getDefaultGlobalWriteConsistencyLevel() {
		return defaultGlobalWriteConsistencyLevel;
	}

	public abstract void loadConsistencyLevelForRead(String columnFamily);

	public abstract void loadConsistencyLevelForWrite(String columnFamily);

	public abstract void reinitDefaultConsistencyLevels();

	public abstract void reinitCurrentConsistencyLevels();

	public abstract ConsistencyLevel getCurrentReadLevel();

	public abstract void setCurrentReadLevel(ConsistencyLevel readLevel);

	public abstract void removeCurrentReadLevel();

	public abstract ConsistencyLevel getCurrentWriteLevel();

	public abstract void setCurrentWriteLevel(ConsistencyLevel writeLevel);

	public abstract void removeCurrentWriteLevel();

	protected void setDefaultGlobalReadConsistencyLevel(ConsistencyLevel defaultGlobalReadConsistencyLevel) {
		this.defaultGlobalReadConsistencyLevel = defaultGlobalReadConsistencyLevel;
	}

	protected void setDefaultGlobalWriteConsistencyLevel(ConsistencyLevel defaultGlobalWriteConsistencyLevel) {
		this.defaultGlobalWriteConsistencyLevel = defaultGlobalWriteConsistencyLevel;
	}

	protected void setReadCfConsistencyLevels(Map<String, ConsistencyLevel> readCfConsistencyLevels) {
		this.readCfConsistencyLevels = readCfConsistencyLevels;
	}

	protected void setWriteCfConsistencyLevels(Map<String, ConsistencyLevel> writeCfConsistencyLevels) {
		this.writeCfConsistencyLevels = writeCfConsistencyLevels;
	}

}
