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
package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.dao.ThriftAbstractDao.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ThriftAbstractSliceIterator<HCOLUMN> implements Iterator<HCOLUMN> {
	private static final Logger log = LoggerFactory.getLogger(ThriftAbstractSliceIterator.class);

	protected boolean reversed;
	protected int count = DEFAULT_LENGTH;
	protected int columns = 0;
	protected AchillesConsistencyLevelPolicy policy;
	protected String columnFamily;
	protected ConsistencyLevel readConsistencyLevelAtInitialization;
	protected Iterator<HCOLUMN> iterator;
	protected Composite start;
	protected ColumnSliceFinish finish;

	protected ThriftAbstractSliceIterator(AchillesConsistencyLevelPolicy policy, String columnFamily, Composite start,
			ColumnSliceFinish finish, boolean reversed, int count) {
		this.policy = policy;
		this.columnFamily = columnFamily;
		this.start = start;
		this.finish = finish;
		this.reversed = reversed;
		this.count = count;
		this.readConsistencyLevelAtInitialization = policy.getCurrentReadLevel();
	}

	public interface ColumnSliceFinish {
		Composite function();
	}

	@Override
	public boolean hasNext() {
		if (iterator == null) {
			iterator = executeSafely(new SafeExecutionContext<Iterator<HCOLUMN>>() {
				@Override
				public Iterator<HCOLUMN> execute() {
					return fetchData();
				}

			});
		} else if (!iterator.hasNext() && columns == count) { // only need to do
																// another query
																// if maximum
																// columns were
																// retrieved

			log.trace("Reload another batch of {} elements for {}", count, type());
			// Exclude start from the query because is has been already fetched
			if (reversed) {
				start.setEquality(ComponentEquality.LESS_THAN_EQUAL);
			} else {
				start.setEquality(ComponentEquality.GREATER_THAN_EQUAL);
			}
			changeQueryRange();
			iterator = executeSafely(new SafeExecutionContext<Iterator<HCOLUMN>>() {
				@Override
				public Iterator<HCOLUMN> execute() {
					return fetchData();
				}

			});

			columns = 0;
		}

		return iterator.hasNext();
	}

	@Override
	public HCOLUMN next() {
		log.trace("Fetching next column from {}", count, type());
		HCOLUMN column = iterator.next();
		resetStartColumn(column);
		columns++;
		return column;
	}

	@Override
	public void remove() {
		iterator.remove();
	}

	protected abstract Iterator<HCOLUMN> fetchData();

	protected abstract void changeQueryRange();

	protected abstract void resetStartColumn(HCOLUMN column);

	public abstract IteratorType type();

	protected <T> T executeWithInitialConsistencyLevel(SafeExecutionContext<T> context) {
		log.trace("Fetching next {} elements with consistency level {} from {}", count,
				readConsistencyLevelAtInitialization != null ? readConsistencyLevelAtInitialization.name() : "QUORUM",
				type());

		T result = null;
		if (readConsistencyLevelAtInitialization != null) {
			log.trace("Save current consistency level {} by {}", policy.getCurrentReadLevel(), type());
			ConsistencyLevel currentReadLevel = policy.getCurrentReadLevel();
			policy.setCurrentReadLevel(readConsistencyLevelAtInitialization);
			policy.loadConsistencyLevelForRead(columnFamily);
			try {
				result = context.execute();
				policy.setCurrentReadLevel(currentReadLevel);
				log.trace("Restore current consistency level {} by {}", currentReadLevel, type());
			} catch (Throwable throwable) {
				policy.reinitCurrentConsistencyLevels();
				policy.reinitDefaultConsistencyLevels();
				log.trace(
						"Exception occurred while fetching next {} elements with consistency level {} in {}. Reset consistency levels",
						count, readConsistencyLevelAtInitialization.name(), type());
				throw new AchillesException(throwable);
			}
		} else {
			result = context.execute();
		}
		return result;

	}

	private <T> T executeSafely(SafeExecutionContext<T> context) {
		try {
			return context.execute();
		} catch (Exception e) {
			policy.reinitCurrentConsistencyLevels();
			policy.reinitDefaultConsistencyLevels();
			log.trace(
					"Exception occurred while fetching next {} elements with consistency level {} in {}. Reset consistency levels",
					count, readConsistencyLevelAtInitialization.name(), type());
			throw new AchillesException(e);
		}
	}

	public enum IteratorType {
		THRIFT_SLICE_ITERATOR, THRIFT_COUNTER_SLICE_ITERATOR;

		@Override
		public String toString() {
			return this.name();
		}
	}
}
