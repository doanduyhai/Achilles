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
package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.entity.metadata.util.PropertyTypeExclude;
import info.archinnov.achilles.entity.metadata.util.PropertyTypeFilter;

import java.util.Set;

import com.google.common.collect.Sets;

public enum PropertyType {

	ID(5), //
	EMBEDDED_ID(10), //
	SIMPLE(10), //
	LIST(10), //
	SET(10), //
	MAP(10), //
	LAZY_SIMPLE(30), //
	COUNTER(30), //
	LAZY_LIST(30), //
	LAZY_SET(30), //
	LAZY_MAP(30);

	private final int flag;

	PropertyType(int flag) {
		this.flag = flag;
	}

	public byte[] flag() {
		return new byte[] { (byte) flag };
	}

	public boolean isLazy() {
		return (this == COUNTER //
				|| this == LAZY_SIMPLE //
				|| this == LAZY_LIST //
				|| this == LAZY_SET //
		|| this == LAZY_MAP //
		);
	}

	public boolean isCounter() {
		return (this == COUNTER);
	}

	public boolean isId() {
		return this == ID || this == EMBEDDED_ID;
	}

	public boolean isEmbeddedId() {
		return this == EMBEDDED_ID;
	}

	public boolean isValidClusteredValueType() {
		return (this == SIMPLE || this == COUNTER);
	}

	public static PropertyTypeFilter counterType = new PropertyTypeFilter(COUNTER);

	public static PropertyTypeFilter eagerType = new PropertyTypeFilter(ID, EMBEDDED_ID, SIMPLE, LIST, SET, MAP);
	public static PropertyTypeFilter lazyType = new PropertyTypeFilter(LAZY_SIMPLE, LAZY_LIST, LAZY_SET, LAZY_MAP,
			COUNTER);

	public static PropertyTypeExclude excludeIdType = new PropertyTypeExclude(ID, EMBEDDED_ID);

	public static PropertyTypeExclude excludeCounterType = new PropertyTypeExclude(COUNTER);

	public static Set<PropertyType> multiValuesNonProxyTypes = Sets.newHashSet(LIST, LAZY_LIST, SET, LAZY_SET, MAP,
			LAZY_MAP);

}
