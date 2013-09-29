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

import me.prettyprint.hector.api.HConsistencyLevel;

public class ThriftConsistencyConvertor {
	private final static Map<ConsistencyLevel, HConsistencyLevel> fromAchillesToHector = new HashMap<ConsistencyLevel, HConsistencyLevel>();

	static {
		fromAchillesToHector.put(ConsistencyLevel.ANY, HConsistencyLevel.ANY);
		fromAchillesToHector.put(ConsistencyLevel.ONE, HConsistencyLevel.ONE);
		fromAchillesToHector.put(ConsistencyLevel.TWO, HConsistencyLevel.TWO);
		fromAchillesToHector.put(ConsistencyLevel.THREE, HConsistencyLevel.THREE);
		fromAchillesToHector.put(ConsistencyLevel.QUORUM, HConsistencyLevel.QUORUM);
		fromAchillesToHector.put(ConsistencyLevel.LOCAL_QUORUM, HConsistencyLevel.LOCAL_QUORUM);
		fromAchillesToHector.put(ConsistencyLevel.EACH_QUORUM, HConsistencyLevel.EACH_QUORUM);
		fromAchillesToHector.put(ConsistencyLevel.ALL, HConsistencyLevel.ALL);
	}

	public static HConsistencyLevel getHectorLevel(ConsistencyLevel achillesLevel) {
		HConsistencyLevel hectorLevel = fromAchillesToHector.get(achillesLevel);
		if (hectorLevel == null) {
			throw new IllegalArgumentException("No matching Hector Consistency Level for Achilles level '"
					+ achillesLevel.name() + "'");
		}

		return hectorLevel;
	}

}
