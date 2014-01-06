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
package info.archinnov.achilles.internal.persistence.metadata;

import info.archinnov.achilles.internal.persistence.metadata.util.PropertyTypeExclude;
import info.archinnov.achilles.internal.persistence.metadata.util.PropertyTypeFilter;

public enum PropertyType {

	ID, //
	EMBEDDED_ID, //
	SIMPLE, //
	LIST, //
	SET, //
	MAP, //
	COUNTER;


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

	public static PropertyTypeExclude excludeIdType = new PropertyTypeExclude(ID, EMBEDDED_ID);
	public static PropertyTypeExclude excludeCounterType = new PropertyTypeExclude(COUNTER);

    public static PropertyTypeExclude excludeIdAndCounterType = new PropertyTypeExclude(ID, EMBEDDED_ID,COUNTER);
}
