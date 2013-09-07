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

import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public abstract class ThriftAbstractClusteredEntityIterator<T> implements
		Iterator<T> {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftAbstractClusteredEntityIterator.class);

	protected Class<T> entityClass;
	private Iterator<?> iterator;
	protected ThriftPersistenceContext context;

	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
	protected ThriftCompositeTransformer transformer = new ThriftCompositeTransformer();

	public ThriftAbstractClusteredEntityIterator(Class<T> entityClass,
			Iterator<?> iterator, ThriftPersistenceContext context) {
		this.entityClass = entityClass;
		this.iterator = iterator;
		this.context = context;
	}

	@Override
	public boolean hasNext() {
		log.trace("Does the iterator {} has next value ? {} ", iterator,
				iterator.hasNext());
		return iterator.hasNext();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(
				"Remove from iterator is not supported. Please use entityManager.remove() instead");
	}

	protected T proxifyClusteredEntity(T target) {

		Set<Method> getters = context.isValueless() ? Sets
				.<Method> newHashSet() : Sets.newHashSet(context.getFirstMeta()
				.getGetter());
		return proxifier.buildProxy(target, context.duplicate(target), getters);
	}
}
