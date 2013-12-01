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
package info.archinnov.achilles.entity.metadata.util;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

public class PropertyTypeFilter implements Predicate<PropertyMeta> {
	private final Set<PropertyType> types;

	public PropertyTypeFilter(PropertyType... types) {
		this.types = Sets.newHashSet(types);
	}

	@Override
	public boolean apply(PropertyMeta pm) {
		return types.contains(pm.type());
	}

};
