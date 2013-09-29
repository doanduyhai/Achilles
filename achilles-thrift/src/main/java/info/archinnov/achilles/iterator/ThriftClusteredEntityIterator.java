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

import info.archinnov.achilles.context.ThriftPersistenceContext;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftClusteredEntityIterator<T> extends ThriftAbstractClusteredEntityIterator<T> {
	private static final Logger log = LoggerFactory.getLogger(ThriftClusteredEntityIterator.class);

	private ThriftAbstractSliceIterator<HColumn<Composite, Object>> sliceIterator;

	public ThriftClusteredEntityIterator(Class<T> entityClass,
			ThriftAbstractSliceIterator<HColumn<Composite, Object>> sliceIterator, ThriftPersistenceContext context) {
		super(entityClass, sliceIterator, context);
		this.sliceIterator = sliceIterator;
	}

	@Override
	public T next() {
		log.trace("Get next clustered entity of type {} ", entityClass.getCanonicalName());
		HColumn<Composite, Object> hColumn = this.sliceIterator.next();
		T target;
		if (context.isValueless()) {
			target = transformer
					.buildClusteredEntityWithIdOnly(entityClass, context, hColumn.getName().getComponents());
		} else {
			target = transformer.buildClusteredEntity(entityClass, context, hColumn);
		}
		return proxifyClusteredEntity(target);

	}

}
