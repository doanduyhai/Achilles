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
package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

import java.util.List;

import com.google.common.base.Optional;

public interface PersistenceContextFactory {

	public static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional.<ConsistencyLevel> absent();
	public static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

	public PersistenceContext newContext(Object entity, Options options);

	public PersistenceContext newContext(Object entity);

	public PersistenceContext newContext(Class<?> entityClass, Object primaryKey, Options options);

	public PersistenceContext newContextForSliceQuery(Class<?> entityClass, List<Object> partitionComponents,
			ConsistencyLevel cl);

}
