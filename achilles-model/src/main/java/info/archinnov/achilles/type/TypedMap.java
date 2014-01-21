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

package info.archinnov.achilles.type;

import java.util.LinkedHashMap;
import java.util.Map;

public class TypedMap extends LinkedHashMap<String, Object> {

	private static final long serialVersionUID = 1L;

	public static TypedMap fromMap(Map<String, Object> source) {
		TypedMap typedMap = new TypedMap();
		typedMap.addAll(source);
		return typedMap;
	}

	@SuppressWarnings("unchecked")
	public <T> T getTyped(String key) {
		T value = null;
		if (super.containsKey(key) && super.get(key) != null) {
			value = (T) super.get(key);
			return value;
		}
		return value;
	}

	public <T> T getTypedOr(String key, T defaultValue) {
		if (super.containsKey(key)) {
			return getTyped(key);
		} else {
			return defaultValue;
		}
	}

	private void addAll(Map<String, Object> source) {
		for (Map.Entry<String, Object> entry : source.entrySet()) {
			super.put(entry.getKey(), entry.getValue());
		}
	}
}
