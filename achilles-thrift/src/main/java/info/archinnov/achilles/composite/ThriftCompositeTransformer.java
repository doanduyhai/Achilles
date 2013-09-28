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

import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import com.google.common.base.Function;

public class ThriftCompositeTransformer {

	private ThriftCompoundKeyMapper compoundKeyMapper = new ThriftCompoundKeyMapper();
	private ThriftEntityMapper mapper = new ThriftEntityMapper();

	public Function<HColumn<Composite, ?>, ?> buildRawValueTransformer() {
		return new Function<HColumn<Composite, ?>, Object>() {
			@Override
			public Object apply(HColumn<Composite, ?> hColumn) {
				return hColumn.getValue();
			}
		};
	}

	// //////////////////// Clustered Entities

	public <T> Function<HColumn<Composite, Object>, T> clusteredEntityTransformer(
			final Class<T> entityClass, final ThriftPersistenceContext context) {
		return new Function<HColumn<Composite, Object>, T>() {
			@Override
			public T apply(HColumn<Composite, Object> hColumn) {
				return buildClusteredEntity(entityClass, context, hColumn);
			}
		};
	}

	public <T> Function<HColumn<Composite, Object>, T> valuelessClusteredEntityTransformer(
			final Class<T> entityClass, final ThriftPersistenceContext context) {
		return new Function<HColumn<Composite, Object>, T>() {
			@Override
			public T apply(HColumn<Composite, Object> hColumn) {
				return buildClusteredEntityWithIdOnly(entityClass, context,
						hColumn.getName().getComponents());
			}
		};
	}

	public <T> Function<HCounterColumn<Composite>, T> counterClusteredEntityTransformer(
			final Class<T> entityClass, final ThriftPersistenceContext context) {
		return new Function<HCounterColumn<Composite>, T>() {
			@Override
			public T apply(HCounterColumn<Composite> hColumn) {
				return buildClusteredEntityWithIdOnly(entityClass, context,
						hColumn.getName().getComponents());
			}
		};
	}

	public <T> T buildClusteredEntity(Class<T> entityClass,
			ThriftPersistenceContext context, HColumn<Composite, Object> hColumn) {
		PropertyMeta pm = context.getFirstMeta();
		Object embeddedId = buildEmbeddedIdFromComponents(context, hColumn
				.getName().getComponents());
		Object clusteredValue = hColumn.getValue();
		Object value = pm.decode(clusteredValue);
		return mapper.createClusteredEntityWithValue(entityClass,
				context.getEntityMeta(), pm, embeddedId, value);
	}

	public <T> T buildClusteredEntityWithIdOnly(Class<T> entityClass,
			ThriftPersistenceContext context, List<Component<?>> components) {
		Object embeddedId = buildEmbeddedIdFromComponents(context, components);
		return mapper.initClusteredEntity(entityClass, context.getEntityMeta(),
				embeddedId);
	}

	private Object buildEmbeddedIdFromComponents(
			ThriftPersistenceContext context, List<Component<?>> components) {
		Object partitionKey = context.getPartitionKey();
		PropertyMeta idMeta = context.getIdMeta();
		return compoundKeyMapper.fromCompositeToEmbeddedId(idMeta, components,
				partitionKey);
	}

}
