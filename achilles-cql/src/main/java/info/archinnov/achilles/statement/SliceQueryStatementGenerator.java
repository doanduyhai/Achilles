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
package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.type.OrderingMode;

import java.util.List;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class SliceQueryStatementGenerator {

	public <T> Statement generateWhereClauseForSelectSliceQuery(CQLSliceQuery<T> sliceQuery, Select select) {

		Where where = select.where();
		List<Object> fixedComponents = sliceQuery.getFixedComponents();
		List<String> componentNames = sliceQuery.getComponentNames();
		String varyingComponentName = sliceQuery.getVaryingComponentName();
		OrderingMode ordering = sliceQuery.getOrdering();

		Object lastStartComp = sliceQuery.getLastStartComponent();
		Object lastEndComp = sliceQuery.getLastEndComponent();

		for (int i = 0; i < fixedComponents.size(); i++) {
			where.and(eq(componentNames.get(i), fixedComponents.get(i)));
		}

		if (ordering == ASCENDING) {

			switch (sliceQuery.getBounding()) {
			case INCLUSIVE_BOUNDS:
				if (lastStartComp != null)
					where.and(gte(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(lte(varyingComponentName, lastEndComp));
				break;
			case EXCLUSIVE_BOUNDS:
				if (lastStartComp != null)
					where.and(gt(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(lt(varyingComponentName, lastEndComp));
				break;
			case INCLUSIVE_START_BOUND_ONLY:
				if (lastStartComp != null)
					where.and(gte(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(lt(varyingComponentName, lastEndComp));
				break;
			case INCLUSIVE_END_BOUND_ONLY:
				if (lastStartComp != null)
					where.and(gt(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(lte(varyingComponentName, lastEndComp));
				break;
			}
		} else // ordering == DESCENDING
		{
			switch (sliceQuery.getBounding()) {
			case INCLUSIVE_BOUNDS:
				if (lastStartComp != null)
					where.and(lte(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(gte(varyingComponentName, lastEndComp));
				break;
			case EXCLUSIVE_BOUNDS:
				if (lastStartComp != null)
					where.and(lt(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(gt(varyingComponentName, lastEndComp));
				break;
			case INCLUSIVE_START_BOUND_ONLY:
				if (lastStartComp != null)
					where.and(lte(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(gt(varyingComponentName, lastEndComp));
				break;
			case INCLUSIVE_END_BOUND_ONLY:
				if (lastStartComp != null)
					where.and(lt(varyingComponentName, lastStartComp));
				if (lastEndComp != null)
					where.and(gte(varyingComponentName, lastEndComp));
				break;
			}

		}
		return where;
	}

	public <T> Statement generateWhereClauseForDeleteSliceQuery(CQLSliceQuery<T> sliceQuery, Delete delete) {
		List<Object> fixedComponents = sliceQuery.getFixedComponents();
		List<String> componentNames = sliceQuery.getComponentNames();

		com.datastax.driver.core.querybuilder.Delete.Where where = delete.where();

		for (int i = 0; i < fixedComponents.size(); i++) {
			where.and(eq(componentNames.get(i), fixedComponents.get(i)));
		}
		return where;
	}

}
