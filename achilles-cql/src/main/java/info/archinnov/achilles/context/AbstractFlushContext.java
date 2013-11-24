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

import info.archinnov.achilles.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.statement.wrapper.SimpleStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;

public abstract class AbstractFlushContext {
	protected DaoContext daoContext;

	protected List<AbstractStatementWrapper> statementWrappers = new ArrayList<AbstractStatementWrapper>();

	protected ConsistencyLevel consistencyLevel;

	public AbstractFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel) {
		this.daoContext = daoContext;
		this.consistencyLevel = consistencyLevel;
	}

	protected AbstractFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
			ConsistencyLevel consistencyLevel) {
		this.statementWrappers = statementWrappers;
		this.daoContext = daoContext;
		this.consistencyLevel = consistencyLevel;
	}

	public void cleanUp() {
		statementWrappers.clear();
		consistencyLevel = null;
	}

	public void pushStatement(BoundStatementWrapper statementWrapper) {
		statementWrappers.add(statementWrapper);
	}

	public void pushStatement(RegularStatementWrapper statementWrapper) {
		statementWrappers.add(statementWrapper);
	}

	public void pushStatement(SimpleStatementWrapper statementWrapper) {
		statementWrappers.add(statementWrapper);
	}

	public ResultSet executeImmediate(BoundStatementWrapper statementWrapper) {
		return daoContext.execute(statementWrapper);
	}

	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	public abstract void startBatch();

	public abstract void flush();

	public abstract void endBatch();

	public abstract FlushType type();

	public abstract AbstractFlushContext duplicate();

	public static enum FlushType {
		BATCH, IMMEDIATE;
	}

	@Override
	public String toString() {
		return type().toString();
	}
}
