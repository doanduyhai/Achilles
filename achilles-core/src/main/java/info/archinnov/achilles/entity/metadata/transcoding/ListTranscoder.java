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
package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

public class ListTranscoder extends AbstractTranscoder {

	public ListTranscoder(ObjectMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	public List<Object> encode(PropertyMeta pm, List<?> entityValue) {
		List<Object> encoded = new ArrayList<Object>();
		for (Object value : entityValue) {
			encoded.add(super.encode(pm, pm.getValueClass(), value));
		}
		return encoded;
	}

	@Override
	public List<Object> decode(PropertyMeta pm, List<?> cassandraValue) {
		List<Object> decoded = new ArrayList<Object>();
		for (Object value : cassandraValue) {
			decoded.add(super.decode(pm, pm.getValueClass(), value));
		}
		return decoded;
	}
}
