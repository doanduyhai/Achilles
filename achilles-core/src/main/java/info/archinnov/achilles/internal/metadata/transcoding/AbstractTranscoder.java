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

import static info.archinnov.achilles.internal.metadata.parsing.PropertyParser.isSupportedType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTranscoder implements DataTranscoder {

	private static final Logger log = LoggerFactory.getLogger(AbstractTranscoder.class);

	protected ObjectMapper objectMapper;
	protected ReflectionInvoker invoker = new ReflectionInvoker();

	public AbstractTranscoder(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public Object encode(PropertyMeta pm, Object entityValue) {
		throw new AchillesException("Transcoder cannot encode value '" + entityValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Object encodeKey(PropertyMeta pm, Object entityValue) {
		throw new AchillesException("Transcoder cannot encode key '" + entityValue + "' for type '" + pm.type().name()
				+ "'");
	}

	@Override
	public List<Object> encode(PropertyMeta pm, List<?> entityValue) {
		throw new AchillesException("Transcoder cannot encode value '" + entityValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Set<Object> encode(PropertyMeta pm, Set<?> entityValue) {
		throw new AchillesException("Transcoder cannot encode value '" + entityValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Map<Object, Object> encode(PropertyMeta pm, Map<?, ?> entityValue) {
		throw new AchillesException("Transcoder cannot encode value '" + entityValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public List<Object> encodeToComponents(PropertyMeta pm, Object compoundKey) {
		throw new AchillesException("Transcoder cannot encode from value '" + compoundKey
				+ "' to components for type '" + pm.type().name() + "'");
	}

	@Override
	public List<Object> encodeToComponents(PropertyMeta pm, List<?> components) {
		throw new AchillesException("Transcoder cannot encode components value '" + components + "'");
	}

	@Override
	public Object decode(PropertyMeta pm, Object cassandraValue) {
		throw new AchillesException("Transcoder cannot decode value '" + cassandraValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Object decodeKey(PropertyMeta pm, Object cassandraValue) {
		throw new AchillesException("Transcoder cannot decode key '" + cassandraValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public List<Object> decode(PropertyMeta pm, List<?> cassandraValue) {
		throw new AchillesException("Transcoder cannot decode value '" + cassandraValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Set<Object> decode(PropertyMeta pm, Set<?> cassandraValue) {
		throw new AchillesException("Transcoder cannot decode value '" + cassandraValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Map<Object, Object> decode(PropertyMeta pm, Map<?, ?> cassandraValue) {
		throw new AchillesException("Transcoder cannot decode value '" + cassandraValue + "' for type '"
				+ pm.type().name() + "'");
	}

	@Override
	public Object decodeFromComponents(PropertyMeta pm, List<?> components) {
		throw new AchillesException("Transcoder cannot decode from components '" + components + "' to value for type '"
				+ pm.type().name() + "'");
	}

	Object encodeInternal(Class<?> sourceType, Object entityValue) {
		log.trace("Encode {} to CQL type {}", entityValue, sourceType);
		if (byte.class.isAssignableFrom(sourceType) || Byte.class.isAssignableFrom(sourceType)) {
			return ByteBuffer.wrap(new byte[] { (byte) entityValue });
		} else if (byte[].class.isAssignableFrom(sourceType)) {
			return ByteBuffer.wrap((byte[]) entityValue);
		} else if (isSupportedType(sourceType)) {
			return entityValue;
		} else if (sourceType.isEnum()) {
			return ((Enum<?>) entityValue).name();
		} else {
			return forceEncodeToJSON(entityValue);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	Object decodeInternal(Class<?> targetType, Object cassandraValue) {
		log.trace("Decode {} from CQL type {}", cassandraValue, targetType);
		if (byte.class.isAssignableFrom(targetType) || Byte.class.isAssignableFrom(targetType)) {
			byte[] byteBuffer = readByteBuffer(cassandraValue);
			if (byteBuffer.length < 1) {
				throw new AchillesException("Error while decoding value '" + cassandraValue + "' to type '"
						+ targetType.getCanonicalName() + "'");
			}
			return byteBuffer[0];
		} else if (byte[].class.isAssignableFrom(targetType)) {
			return readByteBuffer(cassandraValue);
		} else if (isSupportedType(targetType)) {
			return cassandraValue;
		} else if (targetType.isEnum()) {
			return Enum.valueOf((Class) targetType, (String) cassandraValue);
		} else if (cassandraValue instanceof String) {
			return forceDecodeFromJSON((String) cassandraValue, targetType);
		} else {
			throw new AchillesException("Error while decoding value '" + cassandraValue + "' to type '"
					+ targetType.getCanonicalName() + "'");
		}
	}

	private byte[] readByteBuffer(Object cassandraValue) {
		ByteBuffer byteBuffer = (ByteBuffer) cassandraValue;
		byte[] bytes = new byte[byteBuffer.remaining()];
		byteBuffer.get(bytes);
		return bytes;
	}

	@Override
	public String forceEncodeToJSON(Object object) {
		log.trace("Force encode {} to JSON", object);
		String result = null;
		if (object != null) {
			if (object instanceof String) {
				result = String.class.cast(object);
			} else {
				try {

					return this.objectMapper.writeValueAsString(object);
				} catch (Exception e) {
					throw new AchillesException("Error while encoding value '" + object + "'", e);
				}
			}
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T forceDecodeFromJSON(String cassandraValue, Class<T> targetType) {
		log.trace("Force decode {} from JSON to type {}", cassandraValue, targetType);
		T result = null;

		if (cassandraValue != null) {
			if (targetType == String.class) {
				result = (T) cassandraValue;
			} else {
				try {
					return objectMapper.readValue(cassandraValue, targetType);
				} catch (Exception e) {
					throw new AchillesException("Error while decoding value '" + cassandraValue + "' to type '"
							+ targetType.getCanonicalName() + "'", e);
				}
			}
		}
		return result;
	}
}
