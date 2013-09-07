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
package info.archinnov.achilles.test.builders;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.COMPOSITE_SRZ;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;

@SuppressWarnings("unchecked")
public class HColumnTestBuilder {

	public static <V> HColumn<Composite, V> simple(Composite name, V value) {
		return HFactory.createColumn(name, value, COMPOSITE_SRZ,
				(Serializer<V>) ThriftSerializerTypeInferer
						.getSerializer(value));
	}

	public static <V> HColumn<Composite, V> simple(Composite name, V value,
			int ttl) {
		return HFactory.createColumn(name, value, ttl, COMPOSITE_SRZ,
				(Serializer<V>) ThriftSerializerTypeInferer
						.getSerializer(value));
	}

	public static HCounterColumn<Composite> counter(Composite name, Long value) {
		return HFactory.createCounterColumn(name, value, COMPOSITE_SRZ);
	}
}
