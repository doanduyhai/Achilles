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
package info.archinnov.achilles.consistency;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

public class CQLConsistencyConvertor {
	private final static Map<ConsistencyLevel, com.datastax.driver.core.ConsistencyLevel> fromAchillesToCQL = new HashMap<ConsistencyLevel, com.datastax.driver.core.ConsistencyLevel>();

	static {
		fromAchillesToCQL.put(ConsistencyLevel.ANY, com.datastax.driver.core.ConsistencyLevel.ANY);
		fromAchillesToCQL.put(ConsistencyLevel.ONE, com.datastax.driver.core.ConsistencyLevel.ONE);
		fromAchillesToCQL.put(ConsistencyLevel.TWO, com.datastax.driver.core.ConsistencyLevel.TWO);
		fromAchillesToCQL.put(ConsistencyLevel.THREE, com.datastax.driver.core.ConsistencyLevel.THREE);
		fromAchillesToCQL.put(ConsistencyLevel.QUORUM, com.datastax.driver.core.ConsistencyLevel.QUORUM);
		fromAchillesToCQL.put(ConsistencyLevel.LOCAL_QUORUM, com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
		fromAchillesToCQL.put(ConsistencyLevel.EACH_QUORUM, com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		fromAchillesToCQL.put(ConsistencyLevel.ALL, com.datastax.driver.core.ConsistencyLevel.ALL);
	}

	public static com.datastax.driver.core.ConsistencyLevel getCQLLevel(ConsistencyLevel achillesLevel) {
		com.datastax.driver.core.ConsistencyLevel cqlLevel = fromAchillesToCQL.get(achillesLevel);
		if (cqlLevel == null) {
			throw new IllegalArgumentException("No matching Hector Consistency Level for Achilles level '"
					+ achillesLevel.name() + "'");
		}

		return cqlLevel;
	}

}
