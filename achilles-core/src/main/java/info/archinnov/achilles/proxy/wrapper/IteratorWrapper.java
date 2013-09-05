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

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IteratorWrapper extends AbstractWrapper implements
		Iterator<Object> {
	private static final Logger log = LoggerFactory
			.getLogger(IteratorWrapper.class);

	protected Iterator<Object> target;

	public IteratorWrapper(Iterator<Object> target) {
		this.target = target;
	}

	@Override
	public boolean hasNext() {
		return this.target.hasNext();
	}

	@Override
	public Object next() {
		Object value = this.target.next();
		if (isJoin()) {
			log.trace(
					"Build proxy for join entity for property {} of entity class {} upon next() call",
					propertyMeta.getPropertyName(),
					propertyMeta.getEntityClassName());
			PersistenceContext joinContext = context.createContextForJoin(
					propertyMeta.joinMeta(), value);
			return proxifier.buildProxy(value, joinContext);
		} else {
			return value;
		}
	}

	@Override
	public void remove() {
		log.trace(
				"Mark property {} of entity class {} as dirty upon element removal",
				propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		this.target.remove();
		this.markDirty();
	}
}
