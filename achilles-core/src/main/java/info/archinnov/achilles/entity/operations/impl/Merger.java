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

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityMerger;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public interface Merger<CONTEXT extends PersistenceContext> {

	public void merge(CONTEXT context, Map<Method, PropertyMeta> dirtyMap);

	public void cascadeMerge(EntityMerger<CONTEXT> entityMerger,
			CONTEXT context, List<PropertyMeta> joinPMs);
}
