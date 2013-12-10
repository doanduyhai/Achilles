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
package info.archinnov.achilles.statement.prepared;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.type.OrderingMode;

import java.util.List;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class SliceQueryPreparedStatementGenerator {

	public <T> RegularStatement generateWhereClauseForIteratorSliceQuery(CQLSliceQuery<T> sliceQuery, Select select) {

		Where where = select.where();
		List<Object> fixedComponents = sliceQuery.getFixedComponents();
		List<String> componentNames = sliceQuery.getComponentNames();
		String varyingComponentName = sliceQuery.getVaryingComponentName();
		OrderingMode ordering = sliceQuery.getOrdering();

		for (int i = 0; i < fixedComponents.size(); i++) {
			where.and(eq(componentNames.get(i), fixedComponents.get(i)));
		}

		Object lastEndComp = sliceQuery.getLastEndComponent();
		if (ordering == ASCENDING) {
			switch (sliceQuery.getBounding()) {
			case INCLUSIVE_BOUNDS:
			case INCLUSIVE_END_BOUND_ONLY:
				where.and(gt(varyingComponentName, bindMarker()));
				if (lastEndComp != null)
					where.and(lte(varyingComponentName, lastEndComp));
				break;
			case EXCLUSIVE_BOUNDS:
			case INCLUSIVE_START_BOUND_ONLY:
				where.and(gt(varyingComponentName, bindMarker()));
				if (lastEndComp != null)
					where.and(lt(varyingComponentName, lastEndComp));
				break;
			}
		} else // ordering == DESCENDING
		{
			switch (sliceQuery.getBounding()) {
			case INCLUSIVE_BOUNDS:
			case INCLUSIVE_END_BOUND_ONLY:
				where.and(lt(varyingComponentName, bindMarker()));
				if (lastEndComp != null)
					where.and(gte(varyingComponentName, lastEndComp));
				break;
			case EXCLUSIVE_BOUNDS:
			case INCLUSIVE_START_BOUND_ONLY:
				where.and(lt(varyingComponentName, bindMarker()));
				if (lastEndComp != null)
					where.and(gt(varyingComponentName, lastEndComp));
				break;
			}

		}
		return where;
	}
}
