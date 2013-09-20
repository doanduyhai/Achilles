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

import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;

import com.google.common.base.Function;

public class JoinValuesExtractor implements
		Function<PropertyMeta, Pair<List<?>, PropertyMeta>> {

	private Object entity;

	public JoinValuesExtractor(Object entity) {
		this.entity = entity;
	}

	@Override
	public Pair<List<?>, PropertyMeta> apply(PropertyMeta pm) {
		List<Object> joinValues = new ArrayList<Object>();
		Object joinValue = pm.getValueFromField(entity);
		if (joinValue != null) {
			if (pm.isJoinCollection()) {
				joinValues.addAll((Collection) joinValue);
			} else if (pm.isJoinMap()) {
				Map<?, ?> joinMap = (Map<?, ?>) joinValue;
				joinValues.addAll(joinMap.values());
			} else {
				joinValues.add(joinValue);
			}
		}
		return Pair.<List<?>, PropertyMeta> create(joinValues, pm);
	}

}
