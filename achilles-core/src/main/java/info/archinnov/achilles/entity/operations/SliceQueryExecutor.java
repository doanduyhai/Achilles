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
package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

public abstract class SliceQueryExecutor<CONTEXT extends PersistenceContext> {

	public static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional
			.<ConsistencyLevel> absent();
	public static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

	protected EntityProxifier<CONTEXT> proxifier;
	protected ConsistencyLevel defaultReadLevel;

	protected SliceQueryExecutor(EntityProxifier<CONTEXT> proxifier) {
		this.proxifier = proxifier;
	}

	public abstract <T> List<T> get(SliceQuery<T> sliceQuery);

	public abstract <T> Iterator<T> iterator(SliceQuery<T> sliceQuery);

	public abstract <T> void remove(SliceQuery<T> sliceQuery);

	protected abstract <T> CONTEXT buildContextForQuery(SliceQuery<T> sliceQuery);

	protected abstract <T> CONTEXT buildNewContext(SliceQuery<T> sliceQuery,
			T clusteredEntity);

	protected <T> Function<T, T> getProxyTransformer(
			final SliceQuery<T> sliceQuery, final List<Method> getters) {
		return new Function<T, T>() {
			@Override
			public T apply(T clusteredEntity) {
				CONTEXT context = buildNewContext(sliceQuery, clusteredEntity);
				return proxifier.buildProxy(clusteredEntity, context,
						Sets.newHashSet(getters));
			}
		};
	}
}
