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
package info.archinnov.achilles.clustered;

import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class ClusteredEntityFactory {

	private ThriftCompositeTransformer transformer = new ThriftCompositeTransformer();

	public <T> List<T> buildClusteredEntities(Class<T> entityClass, ThriftPersistenceContext context,
			List<HColumn<Composite, Object>> hColumns) {
		if (hColumns.isEmpty()) {
			return new ArrayList<T>();
		} else {
			return buildSimpleClusteredEntities(entityClass, context, hColumns);
		}
	}

	private <T> List<T> buildSimpleClusteredEntities(Class<T> entityClass, ThriftPersistenceContext context,
			List<HColumn<Composite, Object>> hColumns) {
		Function<HColumn<Composite, Object>, T> function;
		if (context.isValueless()) {
			function = transformer.valuelessClusteredEntityTransformer(entityClass, context);
		} else {
			function = transformer.clusteredEntityTransformer(entityClass, context);
		}

		return Lists.transform(hColumns, function);
	}

	public <T> List<T> buildCounterClusteredEntities(Class<T> entityClass, ThriftPersistenceContext context,
			List<HCounterColumn<Composite>> hColumns) {
		Function<HCounterColumn<Composite>, T> function = transformer.counterClusteredEntityTransformer(entityClass,
				context);

		return Lists.transform(hColumns, function);
	}
}
