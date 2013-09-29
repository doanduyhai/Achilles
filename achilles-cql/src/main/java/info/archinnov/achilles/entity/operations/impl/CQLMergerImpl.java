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
package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CQLMergerImpl implements Merger<CQLPersistenceContext> {
	private PropertyMetaComparator comparator = new PropertyMetaComparator();

	@Override
	public void merge(CQLPersistenceContext context, Map<Method, PropertyMeta> dirtyMap) {
		if (dirtyMap.size() > 0) {
			List<PropertyMeta> sortedDirtyMetas = new ArrayList<PropertyMeta>(dirtyMap.values());
			Collections.sort(sortedDirtyMetas, comparator);
			context.pushUpdateStatement(sortedDirtyMetas);
			dirtyMap.clear();
		}
	}

	public static class PropertyMetaComparator implements Comparator<PropertyMeta> {
		@Override
		public int compare(PropertyMeta arg0, PropertyMeta arg1) {
			return arg0.getPropertyName().compareTo(arg1.getPropertyName());
		}

	}
}
