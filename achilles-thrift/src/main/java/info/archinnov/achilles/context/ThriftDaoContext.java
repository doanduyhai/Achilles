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

import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;

import java.util.Map;

public class ThriftDaoContext {
	private final Map<String, ThriftGenericEntityDao> entityDaosMap;
	private final Map<String, ThriftGenericWideRowDao> wideRowDaosMap;
	private final ThriftCounterDao thriftCounterDao;

	public ThriftDaoContext(Map<String, ThriftGenericEntityDao> entityDaosMap,
			Map<String, ThriftGenericWideRowDao> wideRowDaosMap,
			ThriftCounterDao thriftCounterDao) {
		this.entityDaosMap = entityDaosMap;
		this.wideRowDaosMap = wideRowDaosMap;
		this.thriftCounterDao = thriftCounterDao;
	}

	public ThriftCounterDao getCounterDao() {
		return thriftCounterDao;
	}

	public ThriftGenericEntityDao findEntityDao(String columnFamilyName) {
		return entityDaosMap.get(columnFamilyName);
	}

	public ThriftGenericWideRowDao findWideRowDao(String columnFamilyName) {
		return wideRowDaosMap.get(columnFamilyName);
	}
}
