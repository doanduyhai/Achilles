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

public class CQLConsistencyLevelPolicy extends AchillesConsistencyLevelPolicy {

	public CQLConsistencyLevelPolicy(ConsistencyLevel defaultGlobalReadConsistencyLevel,
			ConsistencyLevel defaultGlobalWriteConsistencyLevel, Map<String, ConsistencyLevel> readCfConsistencyLevels,
			Map<String, ConsistencyLevel> writeCfConsistencyLevels) {
		super(defaultGlobalReadConsistencyLevel, defaultGlobalWriteConsistencyLevel, readCfConsistencyLevels,
				writeCfConsistencyLevels);
	}

	@Override
	public void loadConsistencyLevelForRead(String columnFamily) {
		// TODO Auto-generated method stub

	}

	@Override
	public void loadConsistencyLevelForWrite(String columnFamily) {
		// TODO Auto-generated method stub

	}

	@Override
	public void reinitDefaultConsistencyLevels() {
		// TODO Auto-generated method stub

	}

	@Override
	public void reinitCurrentConsistencyLevels() {
		// TODO Auto-generated method stub
	}

	@Override
	public ConsistencyLevel getCurrentReadLevel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCurrentReadLevel(ConsistencyLevel readLevel) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeCurrentReadLevel() {
		// TODO Auto-generated method stub

	}

	@Override
	public ConsistencyLevel getCurrentWriteLevel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCurrentWriteLevel(ConsistencyLevel writeLevel) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeCurrentWriteLevel() {
		// TODO Auto-generated method stub

	}

}
