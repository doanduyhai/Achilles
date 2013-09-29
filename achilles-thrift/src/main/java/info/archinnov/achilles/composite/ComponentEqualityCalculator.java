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
package info.archinnov.achilles.composite;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentEqualityCalculator {
	private static final Logger log = LoggerFactory.getLogger(ComponentEqualityCalculator.class);

	public ComponentEquality[] determineEquality(BoundingMode bounds, OrderingMode ordering) {
		log.trace("Determine component equality with respect to bounding mode {} and ordering mode {}", bounds.name(),
				ordering.name());
		ComponentEquality[] result = new ComponentEquality[2];
		switch (ordering) {
		case ASCENDING:
			switch (bounds) {
			case INCLUSIVE_BOUNDS:
				result[0] = EQUAL;
				result[1] = GREATER_THAN_EQUAL;
				break;
			case EXCLUSIVE_BOUNDS:
				result[0] = GREATER_THAN_EQUAL;
				result[1] = LESS_THAN_EQUAL;
				break;
			case INCLUSIVE_START_BOUND_ONLY:
				result[0] = EQUAL;
				result[1] = LESS_THAN_EQUAL;
				break;
			case INCLUSIVE_END_BOUND_ONLY:
				result[0] = GREATER_THAN_EQUAL;
				result[1] = GREATER_THAN_EQUAL;
				break;
			}
			break;
		case DESCENDING:
			switch (bounds) {
			case INCLUSIVE_BOUNDS:
				result[0] = GREATER_THAN_EQUAL;
				result[1] = EQUAL;
				break;
			case EXCLUSIVE_BOUNDS:
				result[0] = LESS_THAN_EQUAL;
				result[1] = GREATER_THAN_EQUAL;
				break;
			case INCLUSIVE_START_BOUND_ONLY:
				result[0] = GREATER_THAN_EQUAL;
				result[1] = GREATER_THAN_EQUAL;
				break;
			case INCLUSIVE_END_BOUND_ONLY:
				result[0] = LESS_THAN_EQUAL;
				result[1] = EQUAL;
				break;
			}
			break;
		}

		log.trace("For the to bounding mode {} and ordering mode {}, the component equalities should be : {} - {}",
				bounds.name(), ordering.name(), result[0].name(), result[1].name());
		return result;
	}
}
