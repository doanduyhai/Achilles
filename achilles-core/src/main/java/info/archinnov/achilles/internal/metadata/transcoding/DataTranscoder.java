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
package info.archinnov.achilles.internal.metadata.transcoding;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

public interface DataTranscoder {

	// Encode
	public Object encode(PropertyMeta pm, Object entityValue);

	public Object encodeKey(PropertyMeta pm, Object entityValue);

	public List<Object> encode(PropertyMeta pm, List<?> entityValue);

	public Set<Object> encode(PropertyMeta pm, Set<?> entityValue);

	public Map<Object, Object> encode(PropertyMeta pm, Map<?, ?> entityValue);

	public List<Object> encodeToComponents(PropertyMeta pm, Object compoundKey);

	public List<Object> encodeToComponents(PropertyMeta pm, List<?> components);

	public String forceEncodeToJSON(Object object);

	// Decode
	public Object decode(PropertyMeta pm, Object cassandraValue);

	public Object decodeKey(PropertyMeta pm, Object cassandraValue);

	public List<Object> decode(PropertyMeta pm, List<?> cassandraValue);

	public Set<Object> decode(PropertyMeta pm, Set<?> cassandraValue);

	public Map<Object, Object> decode(PropertyMeta pm, Map<?, ?> cassandraValue);

	public Object decodeFromComponents(PropertyMeta pm, List<?> components);

	public <T> T forceDecodeFromJSON(String cassandraValue, Class<T> targetType);
}
