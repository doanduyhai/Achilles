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

package info.archinnov.achilles.configuration;

import info.archinnov.achilles.type.ConsistencyLevel;

public interface ConfigurationParameters {
	String ENTITY_PACKAGES_PARAM = "achilles.entity.packages";

	String OBJECT_MAPPER_FACTORY_PARAM = "achilles.json.object.mapper.factory";
	String OBJECT_MAPPER_PARAM = "achilles.json.object.mapper";

	String CONSISTENCY_LEVEL_READ_DEFAULT_PARAM = "achilles.consistency.read.default";
	String CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM = "achilles.consistency.write.default";
	String CONSISTENCY_LEVEL_READ_MAP_PARAM = "achilles.consistency.read.map";
	String CONSISTENCY_LEVEL_WRITE_MAP_PARAM = "achilles.consistency.write.map";

	String FORCE_CF_CREATION_PARAM = "achilles.ddl.force.column.family.creation";
	String ENSURE_CONSISTENCY_ON_JOIN_PARAM = "achilles.consistency.join.check";

	ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.ONE;
}
