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

import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftCompoundKeyValidator extends CompoundKeyValidator {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftCompoundKeyValidator.class);

	public <K> void validateBoundsForQuery(PropertyMeta propertyMeta, K start,
			K end, OrderingMode ordering) {
		log.trace("Check composites {} / {} with respect to ordering mode {}",
				start, end, ordering.name());
		if (start != null && end != null) {
			if (propertyMeta.isEmbeddedId()) {
				List<Object> startComponents = propertyMeta
						.encodeToComponents(start);
				List<Object> endComponents = propertyMeta
						.encodeToComponents(end);
				validateComponentsForSliceQuery(startComponents, endComponents,
						ordering);
			} else {
				validateComponentsForSliceQuery(Arrays.<Object> asList(start),
						Arrays.<Object> asList(end), ordering);
			}
		}
	}

	@Override
	public void validateComponentsForSliceQuery(
			List<Object> startComponentValues, List<Object> endComponentValues,
			OrderingMode ordering) {
		int indexStart = validateNoHoleAndReturnLastNonNullIndex(startComponentValues);
		int indexEnd = validateNoHoleAndReturnLastNonNullIndex(endComponentValues);

		for (int i = 0; i <= Math.min(indexStart, indexEnd); i++) {
			Object startValue = startComponentValues.get(i);
			Object endValue = endComponentValues.get(i);
			int comparisonResult = comparator.compare(startValue, endValue);

			if (ASCENDING.equals(ordering)) {
				Validator
						.validateTrue(
								comparisonResult <= 0,
								"For slice query with ascending order, start component '"
										+ startValue
										+ "' should be lesser or equal to end component '"
										+ endValue + "'");
				// Stop comparing here
				if (comparisonResult < 0)
					return;
			} else {
				Validator
						.validateTrue(
								comparisonResult >= 0,
								"For slice query with descending order, start component '"
										+ startValue
										+ "' should be greater or equal to end component '"
										+ endValue + "'");
				// Stop comparing here
				if (comparisonResult > 0)
					return;
			}

		}
	}
}
