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
package info.archinnov.achilles.table;

import java.util.Map;
import java.util.Map.Entry;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;

public abstract class TableCreator {
	public static final String TABLE_PATTERN = "[a-zA-Z0-9_]+";
	static final String ACHILLES_DDL_SCRIPT = "ACHILLES_DDL_SCRIPT";

	public void validateOrCreateTables(Map<Class<?>, EntityMeta> entityMetaMap, ConfigurationContext configContext,
			boolean hasCounter) {
		for (Entry<Class<?>, EntityMeta> entry : entityMetaMap.entrySet()) {
			validateOrCreateTableForEntity(entry.getValue(), configContext.isForceColumnFamilyCreation());
		}

		if (hasCounter) {
			validateOrCreateTableForCounter(configContext.isForceColumnFamilyCreation());
		}
	}

	protected abstract void validateOrCreateTableForEntity(EntityMeta entityMeta, boolean forceColumnFamilyCreation);

	protected abstract void validateOrCreateTableForCounter(boolean forceColumnFamilyCreation);

}
