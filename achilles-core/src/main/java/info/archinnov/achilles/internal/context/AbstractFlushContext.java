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
package info.archinnov.achilles.internal.context;

import static info.archinnov.achilles.internal.consistency.ConsistencyConverter.getCQLLevel;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;

public abstract class AbstractFlushContext {
	protected DaoContext daoContext;

	protected List<AbstractStatementWrapper> statementWrappers = new ArrayList<>();
	protected List<AbstractStatementWrapper> counterStatementWrappers = new ArrayList<>();

	protected ConsistencyLevel consistencyLevel;
	protected Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel = Optional.absent();

	public AbstractFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel, Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel) {
		this.daoContext = daoContext;
		this.consistencyLevel = consistencyLevel;
        this.serialConsistencyLevel = serialConsistencyLevel;
    }

	protected AbstractFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
			ConsistencyLevel consistencyLevel,Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel) {
		this.statementWrappers = statementWrappers;
		this.daoContext = daoContext;
		this.consistencyLevel = consistencyLevel;
        this.serialConsistencyLevel = serialConsistencyLevel;
    }

	protected void executeBatch(BatchStatement.Type batchType, List<AbstractStatementWrapper> statementWrappers) {
		if (statementWrappers.size() > 1) {
			BatchStatement batch = new BatchStatement(batchType);
			AbstractStatementWrapper.writeDMLStartBatch(batchType);
			for (AbstractStatementWrapper statementWrapper : statementWrappers) {
                if (statementWrapper instanceof NativeStatementWrapper) {
                    batch.add(((NativeStatementWrapper) statementWrapper).buildParameterizedStatement());
                } else {
                    batch.add(statementWrapper.getStatement());
                }
                statementWrapper.logDMLStatement("\t");
            }
            AbstractStatementWrapper.writeDMLEndBatch(batchType,consistencyLevel);
			if (consistencyLevel != null) {
				batch.setConsistencyLevel(getCQLLevel(consistencyLevel));
			}
            if (serialConsistencyLevel.isPresent()) {
                batch.setSerialConsistencyLevel(serialConsistencyLevel.get());
            }
            daoContext.executeBatch(batch);
		} else if (statementWrappers.size() == 1) {
			daoContext.execute(statementWrappers.get(0));
		}
	}

	public void pushStatement(AbstractStatementWrapper statementWrapper) {
		statementWrappers.add(statementWrapper);
	}

	public void pushCounterStatement(AbstractStatementWrapper statementWrapper) {
		counterStatementWrappers.add(statementWrapper);
	}

	public ResultSet executeImmediate(AbstractStatementWrapper statementWrapper) {
		return daoContext.execute(statementWrapper);
	}

	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	public abstract void startBatch();

	public abstract void flush();

	public abstract void endBatch();

	public abstract FlushType type();

	public abstract AbstractFlushContext duplicate();

	public abstract void triggerInterceptor(EntityMeta meta, Object entity, Event event);

	public static enum FlushType {
		BATCH, IMMEDIATE;
	}

	@Override
	public String toString() {
		return type().toString();
	}
}
