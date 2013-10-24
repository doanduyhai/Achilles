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
package info.archinnov.achilles.compound;

import static info.archinnov.achilles.type.OrderingMode.*;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftSliceQueryValidator extends CompoundKeyValidator {

	private static final Logger log = LoggerFactory.getLogger(ThriftSliceQueryValidator.class);

	public <T> void validateComponentsForSliceQuery(SliceQuery<T> sliceQuery) {
		final List<Object> clusteringsFrom = sliceQuery.getClusteringsFrom();
		final List<Object> clusteringsTo = sliceQuery.getClusteringsTo();
		final int partitionComponentsSize = sliceQuery.partitionComponentsSize();
		final OrderingMode ordering = sliceQuery.getOrdering();

		int indexStart = getLastNonNullIndex(clusteringsFrom);
		int indexEnd = getLastNonNullIndex(clusteringsTo);

		for (int i = partitionComponentsSize; i <= Math.min(indexStart, indexEnd); i++) {
			Object startValue = clusteringsFrom.get(i);
			Object endValue = clusteringsTo.get(i);
			int comparisonResult = comparator.compare(startValue, endValue);

			if (ASCENDING.equals(ordering)) {
				Validator.validateTrue(comparisonResult <= 0, "For slice query with ascending order, start component '"
						+ startValue + "' should be lesser or equal to end component '" + endValue + "'");
				// Stop comparing here
				if (comparisonResult < 0)
					return;
			} else {
				Validator.validateTrue(comparisonResult >= 0,
						"For slice query with descending order, start component '" + startValue
								+ "' should be greater or equal to end component '" + endValue + "'");
				// Stop comparing here
				if (comparisonResult > 0)
					return;
			}

		}
	}
}
