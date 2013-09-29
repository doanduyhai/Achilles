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

import info.archinnov.achilles.proxy.wrapper.builder.MapEntryWrapperBuilder;

import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntryIteratorWrapper extends AbstractWrapper implements Iterator<Entry<Object, Object>> {
	private static final Logger log = LoggerFactory.getLogger(EntryIteratorWrapper.class);

	private Iterator<Entry<Object, Object>> target;

	public EntryIteratorWrapper(Iterator<Entry<Object, Object>> target) {
		this.target = target;
	}

	@Override
	public boolean hasNext() {
		return this.target.hasNext();
	}

	@Override
	public Entry<Object, Object> next() {
		Entry<Object, Object> result = null;
		Entry<Object, Object> entry = this.target.next();
		if (entry != null) {
			log.trace("Build wrapper for next entry of property {} of entity class {}", propertyMeta.getPropertyName(),
					propertyMeta.getEntityClassName());
			result = MapEntryWrapperBuilder.builder(context, entry).dirtyMap(dirtyMap).setter(setter)
					.propertyMeta(propertyMeta).proxifier(proxifier).build();
		}
		return result;
	}

	@Override
	public void remove() {
		log.trace("Mark dirty for property {} of entity class {} upon entry removal", propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		this.target.remove();
		this.markDirty();
	}

}
