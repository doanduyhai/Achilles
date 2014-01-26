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
package info.archinnov.achilles.internal.proxy.wrapper;

import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapEntryWrapper extends AbstractWrapper implements Entry<Object, Object> {
	private static final Logger log = LoggerFactory.getLogger(MapEntryWrapper.class);

	private final Entry<Object, Object> target;

	public MapEntryWrapper(Entry<Object, Object> target) {
		this.target = target;
	}

	@Override
	public Object getKey() {
		return this.target.getKey();
	}

	@Override
	public Object getValue() {
		return this.target.getValue();
	}

	@Override
	public Object setValue(Object value) {
		log.trace("Mark map entry property {} of entity class {} dirty upon element set",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		Object result = this.target.setValue(proxifier.removeProxy(value));
		this.markDirty();
		return result;
	}

	public boolean equals(Entry<Object, Object> entry) {
		Object key = entry.getKey();
		Object value = proxifier.removeProxy(entry.getValue());

		boolean keyEquals = this.target.getKey().equals(key);

		boolean valueEquals = false;
		if (this.target.getValue() == null && value == null) {
			valueEquals = true;
		} else if (this.target.getValue() != null && value != null) {
			valueEquals = this.target.getValue().equals(value);
		}

		return keyEquals && valueEquals;
	}

	@Override
	public int hashCode() {
		Object key = this.target.getKey();
		Object value = this.target.getValue();
		int result = 1;
		result = result * 31 + key.hashCode();
		result = result * 31 + (value == null ? 0 : value.hashCode());
		return result;
	}

	public Entry<Object, Object> getTarget() {
		return target;
	}

}
