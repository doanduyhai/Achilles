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

import static info.archinnov.achilles.dao.ThriftAbstractDao.DEFAULT_LENGTH;
import static info.archinnov.achilles.iterator.ThriftAbstractSliceIterator.IteratorType.THRIFT_SLICE_ITERATOR;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.SliceQuery;

public class ThriftSliceIterator<K, V> extends
		ThriftAbstractSliceIterator<HColumn<Composite, V>> {
	private SliceQuery<K, Composite, V> query;

	public ThriftSliceIterator(AchillesConsistencyLevelPolicy policy,
			String cf, SliceQuery<K, Composite, V> query, Composite start,
			final Composite finish, boolean reversed) {
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public ThriftSliceIterator(AchillesConsistencyLevelPolicy policy,
			String cf, SliceQuery<K, Composite, V> query, Composite start,
			final Composite finish, boolean reversed, int count) {
		this(policy, cf, query, start, new ColumnSliceFinish() {
			@Override
			public Composite function() {
				return finish;
			}
		}, reversed, count);
	}

	public ThriftSliceIterator(AchillesConsistencyLevelPolicy policy,
			String cf, SliceQuery<K, Composite, V> query, Composite start,
			ColumnSliceFinish finish, boolean reversed) {
		this(policy, cf, query, start, finish, reversed, DEFAULT_LENGTH);
	}

	public ThriftSliceIterator(AchillesConsistencyLevelPolicy policy,
			String cf, SliceQuery<K, Composite, V> query, Composite start,
			ColumnSliceFinish finish, boolean reversed, int count) {
		super(policy, cf, start, finish, reversed, count);
		this.query = query;
		this.query.setRange(this.start, this.finish.function(), this.reversed,
				this.count);
	}

	@Override
	protected Iterator<HColumn<Composite, V>> fetchData() {
		return executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HColumn<Composite, V>>>() {
			@Override
			public Iterator<HColumn<Composite, V>> execute() {
				return query.execute().get().getColumns().iterator();
			}
		});
	}

	@Override
	protected void changeQueryRange() {
		query.setRange(start, finish.function(), reversed, count);
	}

	@Override
	protected void resetStartColumn(HColumn<Composite, V> column) {
		start = column.getName();
	}

	@Override
	public IteratorType type() {
		return THRIFT_SLICE_ITERATOR;
	}
}
