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

package info.archinnov.achilles.internal.metadata.parsing.context;

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;

import java.util.Map;

public class ParsingResult {

	private Map<Class<?>, EntityMeta> metaMap;

	private boolean hasSimpleCounter;

	public ParsingResult(Map<Class<?>, EntityMeta> metaMap, boolean hasSimpleCounter) {
		this.metaMap = metaMap;
		this.hasSimpleCounter = hasSimpleCounter;
	}

	public Map<Class<?>, EntityMeta> getMetaMap() {
		return metaMap;
	}

	public boolean isHasSimpleCounter() {
		return hasSimpleCounter;
	}
}
