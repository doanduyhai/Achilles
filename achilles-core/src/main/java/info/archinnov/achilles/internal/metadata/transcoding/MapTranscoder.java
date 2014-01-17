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
package info.archinnov.achilles.internal.metadata.transcoding;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.codehaus.jackson.map.ObjectMapper;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

public class MapTranscoder extends AbstractTranscoder {

	public MapTranscoder(ObjectMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	public Object encode(PropertyMeta pm, Object entityValue) {
		return super.encodeInternal(pm.getValueClass(), entityValue);
	}

	@Override
	public Object encodeKey(PropertyMeta pm, Object entityValue) {
		return super.encodeInternal(pm.getKeyClass(), entityValue);
	}

	@Override
	public Map<Object, Object> encode(PropertyMeta pm, Map<?, ?> entityValue) {
		Map<Object, Object> encoded = new HashMap<Object, Object>();
		for (Entry<?, ?> entry : entityValue.entrySet()) {
			Object encodedKey = super.encodeInternal(pm.getKeyClass(), entry.getKey());
			Object encodedValue = super.encodeInternal(pm.getValueClass(), entry.getValue());
			encoded.put(encodedKey, encodedValue);
		}
		return encoded;
	}

	@Override
	public Object decode(PropertyMeta pm, Object cassandraValue) {
		return super.decodeInternal(pm.getValueClass(), cassandraValue);
	}

	@Override
	public Object decodeKey(PropertyMeta pm, Object cassandraValue) {
		return super.decodeInternal(pm.getKeyClass(), cassandraValue);
	}

	@Override
	public Map<Object, Object> decode(PropertyMeta pm, Map<?, ?> cassandraValue) {
		Map<Object, Object> decoded = new HashMap<Object, Object>();
		for (Entry<?, ?> entry : cassandraValue.entrySet()) {
			Object decodedKey = super.decodeInternal(pm.getKeyClass(), entry.getKey());
			Object decodedValue = super.decodeInternal(pm.getValueClass(), entry.getValue());
			decoded.put(decodedKey, decodedValue);
		}
		return decoded;
	}

}
