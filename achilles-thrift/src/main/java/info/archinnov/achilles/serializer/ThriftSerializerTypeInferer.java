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
package info.archinnov.achilles.serializer;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;

public class ThriftSerializerTypeInferer {

	public static <T> Serializer<T> getSerializer(Class<?> valueClass) {
		if (valueClass == null) {
			return null;
		}

		Serializer<T> serializer = SerializerTypeInferer.getSerializer(valueClass);

		if (serializer == null || serializer.equals(ThriftSerializerUtils.OBJECT_SRZ)) {
			return SerializerTypeInferer.getSerializer(String.class);
		} else {
			return serializer;
		}
	}

	public static <T> Serializer<T> getSerializer(Object value) {
		if (value == null) {
			return null;
		}

		Class<?> valueClass = value.getClass();

		return getSerializer(valueClass);
	}
}
