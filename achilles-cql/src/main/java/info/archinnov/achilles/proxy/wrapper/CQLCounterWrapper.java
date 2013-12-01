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

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

public class CQLCounterWrapper implements Counter {

	private transient PersistenceContext context;
	private transient PropertyMeta counterMeta;
	private transient boolean clusteredCounter;

	public CQLCounterWrapper(PersistenceContext context, PropertyMeta counterMeta) {
		this.context = context;
		this.counterMeta = counterMeta;
		this.clusteredCounter = context.getEntityMeta().isClusteredCounter();
	}

	@Override
	public Long get() {
		ConsistencyLevel readLevel = getReadRuntimeConsistencyIfPossible();
		if (clusteredCounter)
			return context.getClusteredCounter(counterMeta, readLevel);
		else
			return context.getSimpleCounter(counterMeta, readLevel);
	}

	@Override
	public Long get(ConsistencyLevel readLevel) {
		if (clusteredCounter)
			return context.getClusteredCounter(counterMeta, readLevel);
		else
			return context.getSimpleCounter(counterMeta, readLevel);
	}

	@Override
	public void incr() {
		ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
		if (clusteredCounter)
			context.incrementClusteredCounter(1L, writeLevel);
		else
			context.incrementSimpleCounter(counterMeta, 1L, writeLevel);
	}

	@Override
	public void incr(ConsistencyLevel writeLevel) {
		if (clusteredCounter)
			context.incrementClusteredCounter(1L, writeLevel);
		else
			context.incrementSimpleCounter(counterMeta, 1L, writeLevel);
	}

	@Override
	public void incr(Long increment) {
		ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
		if (clusteredCounter)
			context.incrementClusteredCounter(increment, writeLevel);
		else
			context.incrementSimpleCounter(counterMeta, increment, writeLevel);
	}

	@Override
	public void incr(Long increment, ConsistencyLevel writeLevel) {
		if (clusteredCounter)
			context.incrementClusteredCounter(increment, writeLevel);
		else
			context.incrementSimpleCounter(counterMeta, increment, writeLevel);
	}

	@Override
	public void decr() {
		ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
		if (clusteredCounter)
			context.decrementClusteredCounter(1L, writeLevel);
		else
			context.decrementSimpleCounter(counterMeta, 1L, writeLevel);
	}

	@Override
	public void decr(ConsistencyLevel writeLevel) {
		if (clusteredCounter)
			context.decrementClusteredCounter(1L, writeLevel);
		else
			context.decrementSimpleCounter(counterMeta, 1L, writeLevel);
	}

	@Override
	public void decr(Long decrement) {
		ConsistencyLevel writeLevel = getWriteRuntimeConsistencyIfPossible();
		if (clusteredCounter)
			context.decrementClusteredCounter(decrement, writeLevel);
		else
			context.decrementSimpleCounter(counterMeta, decrement, writeLevel);
	}

	@Override
	public void decr(Long decrement, ConsistencyLevel writeLevel) {
		if (clusteredCounter)
			context.decrementClusteredCounter(decrement, writeLevel);
		else
			context.decrementSimpleCounter(counterMeta, decrement, writeLevel);
	}

	private ConsistencyLevel getReadRuntimeConsistencyIfPossible() {
		return context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get() : counterMeta
				.getReadConsistencyLevel();
	}

	private ConsistencyLevel getWriteRuntimeConsistencyIfPossible() {
		return context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get() : counterMeta
				.getWriteConsistencyLevel();
	}
}
